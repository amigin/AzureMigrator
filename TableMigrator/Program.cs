using System;
using System.Threading;

namespace TableMigrator
{
    class Program
    {

        static void Main(string[] args)
        {

            var srcConnString = Environment.GetEnvironmentVariable("SrcConnString");

            if (string.IsNullOrEmpty(srcConnString))
                throw new Exception("Please specify: 'SrcConnString' env variable");

            var destConnString = Environment.GetEnvironmentVariable("DestConnString");

            if (string.IsNullOrEmpty(destConnString))
                throw new Exception("Please specify: 'DestConnString' env variable");

            var orchestrator = new CopyPasteOrchestrator(srcConnString, destConnString, 3);

            var runningTask = orchestrator.Start();

            runningTask.Wait();


            Console.WriteLine("Finished...");

            Thread.Sleep(30000);

        }

    }

}