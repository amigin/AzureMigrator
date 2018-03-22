using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace TableMigrator
{


    public class CopyPasteEngine
    {
        public readonly TableEntitySdk SrcTable;
        public readonly TableEntitySdk DestTable;
        private readonly Queue<DynamicTableEntity[]> _queue = new Queue<DynamicTableEntity[]>();


        public bool Stopped { get; set; }







        private Task _readTask;
        private Task _writeTask;


        public CopyPasteEngine(TableEntitySdk srcTable, TableEntitySdk destTable)
        {
            SrcTable = srcTable;
            DestTable = destTable;

            _readTask = ReadThreadAsync();
            _writeTask = WriteThreadAsync();

        }

        public async Task ReadThreadAsync()
        {
            await SrcTable.GetEntitiesByChunkAsync(_queue.WriteAsync);
            _queue.WriteEof();
        }


        public async Task WriteThreadAsync()
        {

            while (true)
            {
                var entities = await _queue.ReadAsync();

                if (entities == null)
                    break;

                await DestTable.InsertAsync(entities);
            }

            Stopped = true;
        }

    }


    public static class CopyPasteExtentions
    {

        public static void WriteEof(this Queue<DynamicTableEntity[]> queue)
        {
            lock (queue)
            {
                queue.Enqueue(null);
            }
        }

        public static async Task WriteAsync(this Queue<DynamicTableEntity[]> queue, DynamicTableEntity[] entities)
        {
            while (true)
            {

                lock (queue)
                {
                    if (queue.Count < 5)
                    {
                        queue.Enqueue(entities);
                        return;
                    }
                }

                await Task.Delay(500);

            }

        }

        public static async Task<DynamicTableEntity[]> ReadAsync(this Queue<DynamicTableEntity[]> queue)
        {

            while (true)
            {
                lock (queue)
                {
                    if (queue.Count > 0)
                        return queue.Dequeue();
                }

                await Task.Delay(100);
            }
        }
    }


}