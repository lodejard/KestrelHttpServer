// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Microsoft.AspNet.Server.Kestrel.Filter
{
    internal class LoggingStream : Stream
    {
        private readonly Stream _inner;
        private readonly ILogger _logger;

        public LoggingStream(Stream inner, ILogger logger)
        {
            _inner = inner;
            _logger = logger;
        }

        public override bool CanRead
        {
            get
            {
                return _inner.CanRead;
            }
        }

        public override bool CanSeek
        {
            get
            {
                return _inner.CanSeek;
            }
        }

        public override bool CanWrite
        {
            get
            {
                return _inner.CanWrite;
            }
        }

        public override long Length
        {
            get
            {
                return _inner.Length;
            }
        }

        public override long Position
        {
            get
            {
                return _inner.Position;
            }

            set
            {
                _inner.Position = value;
            }
        }

        public override void Flush()
        {
            _inner.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int read = _inner.Read(buffer, offset, count);
            _logger.LogVerbose($"Read[{read}]" + PrintBuffer(buffer, offset, read));
            return read;
        }

        public async override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int read = await _inner.ReadAsync(buffer, offset, count, cancellationToken);
            _logger.LogVerbose($"ReadAsync[{read}]" + PrintBuffer(buffer, offset, read));
            return read;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _inner.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _inner.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _logger.LogVerbose($"Write[{count}]" + PrintBuffer(buffer, offset, count));
            _inner.Write(buffer, offset, count);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            _logger.LogVerbose($"WriteAsync[{count}]" + PrintBuffer(buffer, offset, count));
            return _inner.WriteAsync(buffer, offset, count, cancellationToken);
        }

        private string PrintBuffer(byte[] buffer, int offset, int read)
        {
            var builder = new StringBuilder();
            // Write the hex
            for (int i = offset; i < read + offset; i++)
            {
                builder.Append(buffer[i].ToString("x"));
                builder.Append(" ");
            }
            builder.AppendLine();
            // Write the bytes as if they were ASCII
            for (int i = offset; i < read + offset; i++)
            {
                builder.Append((char)buffer[i]);
            }
            return builder.ToString();
        }
    }
}
