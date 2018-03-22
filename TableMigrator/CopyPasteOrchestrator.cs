using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TableMigrator
{

    public class CopyPasteOrchestrator
    {
        private readonly string _srcConnString;
        private readonly string _destConnString;
        private readonly int _slotsCount;

        private readonly Dictionary<string, CopyPasteEngine> _activeSlots = new Dictionary<string, CopyPasteEngine>();
        private readonly Dictionary<string, CopyPasteEngine> _doneSlots = new Dictionary<string, CopyPasteEngine>();

        public CopyPasteOrchestrator(string srcConnString, string destConnString, int slotsCount)
        {
            _srcConnString = srcConnString;
            _destConnString = destConnString;
            _slotsCount = slotsCount;
        }


        private void FillSlots(List<TableEntitySdk> srcTables)
        {

            while (_activeSlots.Count < _slotsCount)
            {

                if (srcTables.Count == 0)
                    return;

                var srcTable = srcTables[0];
                srcTables.RemoveAt(0);

                var destTable = new TableEntitySdk(_destConnString, srcTable.CloudTable.Name);
                var engine = new CopyPasteEngine(srcTable, destTable);
                _activeSlots.Add(srcTable.CloudTable.Name, engine);
            }

        }


        private void ClearSlots()
        {

            var slots = _activeSlots.Values.ToArray();

            foreach (var slot in slots)
            {
                if (!slot.Stopped) continue;

                _doneSlots.Add(slot.SrcTable.CloudTable.Name, slot);
                _activeSlots.Remove(slot.SrcTable.CloudTable.Name);
            }

        }

        private void ShowConsole()
        {

            Console.Clear();

            foreach (var slot in _activeSlots.Values)
            {

                Console.WriteLine("Table: " + slot.SrcTable.CloudTable.Name.PadRight(40) + " R:" + slot.SrcTable.Count.ToString().PadRight(20)
                              + "W:" + slot.DestTable.Count.ToString().PadRight(20)
                              + "E:" + slot.DestTable.Errors.ToString().PadRight(20));

            }

            Console.WriteLine("----- Done -----");

            foreach (var slot in _doneSlots.Values)
            {

                Console.WriteLine("Table: " + slot.SrcTable.CloudTable.Name.PadRight(40) + " R:" + slot.SrcTable.Count.ToString().PadRight(20)
                                  + "W:" + slot.DestTable.Count.ToString().PadRight(20)
                                  + "E:" + slot.DestTable.Errors.ToString().PadRight(20));

            }


        }

        public async Task TheThreadAsync()
        {
            var srcTables = _srcConnString.GetTables().ToList();


            while (true)
            {
                ClearSlots();
                FillSlots(srcTables);
                ShowConsole();

                if (_activeSlots.Count == 0)
                    break;

                await Task.Delay(1000);
            }



        }

        public Task Start()
        {
            return TheThreadAsync();
        }

    }

}