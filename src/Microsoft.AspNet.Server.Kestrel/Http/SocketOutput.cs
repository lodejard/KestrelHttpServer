// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNet.Server.Kestrel.Infrastructure;
using Microsoft.AspNet.Server.Kestrel.Networking;

namespace Microsoft.AspNet.Server.Kestrel.Http
{
    public class SocketOutput : ISocketOutput
    {
        private const int _maxPendingWrites = 3;
        private const int _maxBytesPreCompleted = 65536;

        private readonly KestrelThread _thread;
        private readonly UvStreamHandle _socket;
        private readonly MemoryPool2 _memory;
        private readonly Connection _connection;
        private readonly long _connectionId;
        private readonly IKestrelTrace _log;

        // This locks access to to all of the below fields
        private readonly object _lockObj = new object();

        // The number of write operations that have been scheduled so far
        // but have not completed.
        private int _writesPending = 0;

        private int _numBytesPreCompleted = 0;
        private Exception _lastWriteError;
        private WriteContext _nextWriteContext;
        private readonly Queue<TaskCompletionSource<object>> _tasksPending;

        public SocketOutput(
            KestrelThread thread,
            UvStreamHandle socket,
            MemoryPool2 memory,
            Connection connection,
            long connectionId,
            IKestrelTrace log)
        {
            _thread = thread;
            _socket = socket;
            _memory = memory;
            _connection = connection;
            _connectionId = connectionId;
            _log = log;
            _tasksPending = new Queue<TaskCompletionSource<object>>();
        }

        public Task WriteAsync(
            ArraySegment<byte> buffer,
            bool immediate = true,
            bool socketShutdownSend = false,
            bool socketDisconnect = false)
        {
            _log.ConnectionWrite(_connectionId, buffer.Count);

            TaskCompletionSource<object> tcs = null;
            var blocks = CopyBuffer(buffer);

            lock (_lockObj)
            {
                if (_nextWriteContext == null)
                {
                    _nextWriteContext = new WriteContext(this);
                }
                if (socketShutdownSend)
                {
                    _nextWriteContext.SocketShutdownSend = true;
                }
                if (socketDisconnect)
                {
                    _nextWriteContext.SocketDisconnect = true;
                }

                for (int i = 0; i < blocks.Length - 1; i++)
                {
                    _nextWriteContext.Blocks.Enqueue(blocks[i]);
                }

                if (blocks.Length > 0)
                {
                    var block = blocks[blocks.Length - 1];

                    _nextWriteContext.Blocks.Enqueue(block);

                    if (!immediate)
                    {
                        // immediate==false calls always return complete tasks, because there is guaranteed
                        // to be a subsequent immediate==true call which will go down one of the previous code-paths
                        _numBytesPreCompleted += buffer.Count;
                    }
                    else if (_lastWriteError == null &&
                            _tasksPending.Count == 0 &&
                            _numBytesPreCompleted + buffer.Count <= _maxBytesPreCompleted)
                    {
                        // Complete the write task immediately if all previous write tasks have been completed,
                        // the buffers haven't grown too large, and the last write to the socket succeeded.
                        _numBytesPreCompleted += buffer.Count;
                    }
                    else
                    {
                        // immediate write, which is not eligable for instant completion above
                        tcs = new TaskCompletionSource<object>(buffer.Count);
                        _tasksPending.Enqueue(tcs);
                    }
                }

                if (_writesPending < _maxPendingWrites && immediate)
                {
                    try
                    {
                        ScheduleWrite();
                    }
                    catch
                    {
                        _nextWriteContext.Blocks.Dequeue();

                        foreach (var block in blocks)
                        {
                            block.Unpin();
                            _memory.Return(block);
                        }

                        throw;
                    }

                    _writesPending++;
                }
            }

            // Return TaskCompletionSource's Task if set, otherwise completed Task 
            return tcs?.Task ?? TaskUtilities.CompletedTask;
        }

        public void End(ProduceEndType endType)
        {
            switch (endType)
            {
                case ProduceEndType.SocketShutdownSend:
                    WriteAsync(default(ArraySegment<byte>),
                        immediate: true,
                        socketShutdownSend: true,
                        socketDisconnect: false);
                    break;
                case ProduceEndType.SocketDisconnect:
                    WriteAsync(default(ArraySegment<byte>),
                        immediate: true,
                        socketShutdownSend: false,
                        socketDisconnect: true);
                    break;
            }
        }

        private MemoryPoolBlock2[] CopyBuffer(ArraySegment<byte> buffer)
        {
            var maxBlockSize = 2048;

            var remaining = buffer.Count;
            var srcOffset = buffer.Offset;
            var numBlocks = (remaining + maxBlockSize - 1) / maxBlockSize;
            var buffers = new MemoryPoolBlock2[numBlocks];
            var i = 0;

            while (remaining > 0)
            {
                var block = _memory.Lease(2048);

                var limit = Math.Min(2048, remaining);
                Buffer.BlockCopy(buffer.Array, srcOffset, block.Array, block.Start, limit);
                block.End += limit;
                srcOffset += limit;
                remaining -= limit;

                buffers[i++] = block;
            }

            return buffers;
        }

        private void ScheduleWrite()
        {
            _thread.Post(_this => _this.WriteAllPending(), this);
        }

        // This is called on the libuv event loop
        private void WriteAllPending()
        {
            WriteContext writingContext;

            lock (_lockObj)
            {
                if (_nextWriteContext != null)
                {
                    writingContext = _nextWriteContext;
                    _nextWriteContext = null;
                }
                else
                {
                    _writesPending--;
                    return;
                }
            }

            try
            {
                writingContext.DoWriteIfNeeded();
            }
            catch
            {
                lock (_lockObj)
                {
                    // Lock instead of using Interlocked.Decrement so _writesSending
                    // doesn't change in the middle of executing other synchronized code.
                    _writesPending--;
                }

                foreach (var block in writingContext.Blocks)
                {
                    block.Unpin();
                    _memory.Return(block);
                }

                throw;
            }
        }

        // This is called on the libuv event loop
        private void OnWriteCompleted(Queue<MemoryPoolBlock2> writtenBuffers, int status, Exception error)
        {
            _log.ConnectionWriteCallback(_connectionId, status);

            if (error != null)
            {
                _lastWriteError = new IOException(error.Message, error);

                // Abort the connection for any failed write.
                _connection.Abort();
            }

            lock (_lockObj)
            {
                if (_nextWriteContext != null)
                {
                    ScheduleWrite();
                }
                else
                {
                    _writesPending--;
                }

                foreach (var writeBuffer in writtenBuffers)
                {
                    // _numBytesPreCompleted can temporarily go negative in the event there are
                    // completed writes that we haven't triggered callbacks for yet.
                    _numBytesPreCompleted -= writeBuffer.End - writeBuffer.Start;
                    writeBuffer.Unpin();
                    _memory.Return(writeBuffer);
                }
                
                // bytesLeftToBuffer can be greater than _maxBytesPreCompleted
                // This allows large writes to complete once they've actually finished.
                var bytesLeftToBuffer = _maxBytesPreCompleted - _numBytesPreCompleted;
                while (_tasksPending.Count > 0 &&
                       (int)(_tasksPending.Peek().Task.AsyncState) <= bytesLeftToBuffer)
                {
                    var tcs = _tasksPending.Dequeue();
                    var bytesToWrite = (int)tcs.Task.AsyncState;

                    _numBytesPreCompleted += bytesToWrite;
                    bytesLeftToBuffer -= bytesToWrite;

                    if (_lastWriteError == null)
                    {
                        ThreadPool.QueueUserWorkItem(
                            (o) => ((TaskCompletionSource<object>)o).SetResult(null), 
                            tcs);
                    }
                    else
                    {
                        // error is closure captured 
                        ThreadPool.QueueUserWorkItem(
                            (o) => ((TaskCompletionSource<object>)o).SetException(_lastWriteError), 
                            tcs);
                    }
                }

                // Now that the while loop has completed the following invariants should hold true:
                Debug.Assert(_numBytesPreCompleted >= 0);
            }
        }

        void ISocketOutput.Write(ArraySegment<byte> buffer, bool immediate)
        {
            var task = WriteAsync(buffer, immediate);

            if (task.Status == TaskStatus.RanToCompletion)
            {
                return;
            }
            else
            {
                task.GetAwaiter().GetResult();
            }
        }

        Task ISocketOutput.WriteAsync(ArraySegment<byte> buffer, bool immediate, CancellationToken cancellationToken)
        {
            return WriteAsync(buffer, immediate);
        }

        private class WriteContext
        {
            public SocketOutput Self;

            public Queue<MemoryPoolBlock2> Blocks;
            public bool SocketShutdownSend;
            public bool SocketDisconnect;

            public int WriteStatus;
            public Exception WriteError;

            public int ShutdownSendStatus;

            public WriteContext(SocketOutput self)
            {
                Self = self;
                Blocks = new Queue<MemoryPoolBlock2>();
            }

            /// <summary>
            /// First step: initiate async write if needed, otherwise go to next step
            /// </summary>
            public void DoWriteIfNeeded()
            {
                if (Blocks.Count == 0 || Self._socket.IsClosed)
                {
                    DoShutdownIfNeeded();
                    return;
                }

                var buffers = new MemoryPoolBlock2[Blocks.Count];

                var i = 0;
                foreach (var block in Blocks)
                {
                    buffers[i++] = block;
                }

                var writeReq = new UvWriteReq(Self._log);
                writeReq.Init(Self._thread.Loop);
                writeReq.Write(Self._socket, new ArraySegment<MemoryPoolBlock2>(buffers), (_writeReq, status, error, state) =>
                {
                    _writeReq.Dispose();
                    var _this = (WriteContext)state;
                    _this.WriteStatus = status;
                    _this.WriteError = error;
                    _this.DoShutdownIfNeeded();
                }, this);
            }

            /// <summary>
            /// Second step: initiate async shutdown if needed, otherwise go to next step
            /// </summary>
            public void DoShutdownIfNeeded()
            {
                if (SocketShutdownSend == false || Self._socket.IsClosed)
                {
                    DoDisconnectIfNeeded();
                    return;
                }

                var shutdownReq = new UvShutdownReq(Self._log);
                shutdownReq.Init(Self._thread.Loop);
                shutdownReq.Shutdown(Self._socket, (_shutdownReq, status, state) =>
                {
                    _shutdownReq.Dispose();
                    var _this = (WriteContext)state;
                    _this.ShutdownSendStatus = status;

                    _this.Self._log.ConnectionWroteFin(Self._connectionId, status);

                    _this.DoDisconnectIfNeeded();
                }, this);
            }

            /// <summary>
            /// Third step: disconnect socket if needed, otherwise this work item is complete
            /// </summary>
            public void DoDisconnectIfNeeded()
            {
                if (SocketDisconnect == false || Self._socket.IsClosed)
                {
                    Complete();
                    return;
                }

                Self._socket.Dispose();
                Self._log.ConnectionStop(Self._connectionId);
                Complete();
            }

            public void Complete()
            {
                Self.OnWriteCompleted(Blocks, WriteStatus, WriteError);
            }
        }
    }
}
