using System;

namespace Redshift
{
    class Program
    {
        private static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("no arguments provided. Please use help");
                Console.WriteLine("Press any key to exit.");
                Console.ReadKey();

                return;
            }
            if (args[0].ToLower() == "help" || args[0].ToLower() == "-help" || args[0] == "h" || args[0] == "-h" || args[0] == "--h")
            {
                Console.WriteLine("[Function  -arg1 -arg2]");
                Console.WriteLine("");                
                Console.WriteLine("Functions: Describe, Clone, Terminate, Restore");
                Console.WriteLine("--------------");
                Console.WriteLine("Describe - NO ARGUMENTS");
                Console.WriteLine("Clone - ClusterIdentifier, ManualSnapShotIdentifier");
                Console.WriteLine("Terminate - ClusterIdentifier");
                Console.WriteLine("Restore - ClusterIdentifier, ManualSnapShotIdentifier");
                Console.WriteLine("--------------");
                Console.WriteLine("Examples:");
                Console.WriteLine("Redshift.exe Describe");
                Console.WriteLine("Redshift.exe Clone leo-plspt-redshif, manual-leo-plspt-redshif");
                Console.WriteLine("Redshift.exe Terminate leo-plspt-redshif");
                Console.WriteLine("Redshift.exe Restore leo-plspt-redshif, manual-leo-plspt-redshif");

                return;
            }
            if (args[0].ToLower() == "describe")
            {
                Describe();
                return;
            }
            if (args[0].ToLower() == "clone" && args.Length == 3)
            {
                CloneRedShiftCluster(args[1], args[2]);
                return;
            }
            if (args[0].ToLower() == "terminate" && args.Length == 2)
            {
                TerminateRedShiftCluster(args[1]);
                return;
            }
            if (args[0].ToLower() == "restore" && args.Length == 3)
            {
                RestoreRedShiftCluster(args[1], args[2]);
                return;
            }

            Console.WriteLine("Please use help argument ");
            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

       
        static void Describe()
        {
            RedShiftService redShift = new RedShiftService();
            redShift.DescribeRedShiftClusters();
            Console.WriteLine("Describe done...");
            Console.ReadKey();
        }


        static void CloneRedShiftCluster(string manualSnapShotIdentifier, string clusterIdentifier)
        {
            RedShiftService redShift = new RedShiftService();
            redShift.CloneRedShiftClustersToSnapshot(manualSnapShotIdentifier, clusterIdentifier);
            Console.WriteLine("Clone done...");
            Console.ReadKey();
        }

        static void TerminateRedShiftCluster(string clusterIdentifier)
        {
            RedShiftService redShift = new RedShiftService();
            redShift.TerminateCluster(clusterIdentifier);
            Console.WriteLine("Termination done...");
            Console.ReadKey();
        }

        static void RestoreRedShiftCluster(string manualSnapShotIdentifier, string clusterIdentifier)
        {
            RedShiftService redShift = new RedShiftService();
            redShift.RestoreRedShiftClustersFromSnepshot(clusterIdentifier, manualSnapShotIdentifier);
            Console.WriteLine("Restore done...");
            Console.ReadKey();
        }






    }
}
