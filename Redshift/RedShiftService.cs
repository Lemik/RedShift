using System;
using System.Collections.Generic;
using System.Threading;
using Amazon;
using Amazon.Redshift;
using Amazon.Redshift.Model;

namespace Redshift
{
    internal class RedShiftService
    {
        /// <summary>
        /// SandBox 
        /// </summary>
        private const string AwsAccessKeyId = "";
        private const string AwsSecretAccessKey = "";

        private IAmazonRedshift GetRedShiftClient()
        {
            AmazonRedshiftClient client = new AmazonRedshiftClient(AwsAccessKeyId, AwsSecretAccessKey,
                RegionEndpoint.USWest2);
            return client;
        }

        /// <summary>
        /// Returns properties of provisioned clusters including general cluster properties, cluster database properties,
        ///  maintenance and backup properties, and security and access properties. This operation supports pagination.
        ///  For more information about managing clusters, go to Amazon Redshift Clusters in the Amazon Redshift Cluster Management Guide.
        /// </summary>
        public void DescribeRedShiftClusters()
        {
            using (IAmazonRedshift redshiftClient = GetRedShiftClient())
            {
                DescribeClustersResponse describeClustersResponse = redshiftClient.DescribeClusters();
                List<Cluster> redshiftClusters = describeClustersResponse.Clusters;
                foreach (Cluster cluster in redshiftClusters)
                {
                    Console.WriteLine("Cluster id: {0}", cluster.ClusterIdentifier);
                    Console.WriteLine("Cluster status: {0}", cluster.ClusterStatus);
                    Console.WriteLine("Cluster creation date: {0}", cluster.ClusterCreateTime);
                    Console.WriteLine("Cluster DB name: {0}", cluster.DBName);
                }
            }
        }

        /// <summary>
        /// To create the cluster in Virtual Private Cloud (VPC), you must provide a cluster subnet group name. 
        /// The cluster subnet group identifies the subnets of your VPC that Amazon Redshift uses when creating the cluster. 
        /// For more information about managing clusters, go to Amazon Redshift Clusters in the Amazon Redshift Cluster Management Guide.
        /// </summary>
        /// <param name="clusterIdentifier">A unique identifier for the cluster. You use this identifier to refer to the cluster for any subsequent cluster operations such as deleting or modifying. The identifier also appears in the Amazon Redshift console.</param>
        /// <param name="masterUsername">The user name associated with the master user account for the cluster that is being created.</param>
        /// <param name="masterUserPassword">The password associated with the master user account for the cluster that is being created.</param>
        /// <param name="nodeType">The node type to be provisioned for the cluster. For information about node types, go to Working with Clusters in the Amazon Redshift Cluster Management Guide.</param>
        /// <param name="dbName">The name of the first database to be created when the cluster is created.</param>
        /// <param name="port">The port number on which the cluster accepts incoming connections. (Default:5439)</param>
        /// <param name="clusterType">The type of the cluster. When cluster type is specified as  (Default:single-node)</param>
        /// http://docs.aws.amazon.com/redshift/latest/APIReference/API_CreateCluster.html
        public void StartNewCluster(string clusterIdentifier, string masterUsername, string masterUserPassword,
            string nodeType, string dbName, int port = 5439, string clusterType = "single-node")
        {
            using (IAmazonRedshift redshiftClient = GetRedShiftClient())
            {
                try
                {
                    CreateClusterRequest createClusterRequest = new CreateClusterRequest();

                    createClusterRequest.ClusterIdentifier = clusterIdentifier;
                    createClusterRequest.DBName = dbName;
                    createClusterRequest.MasterUsername = masterUsername;
                    createClusterRequest.MasterUserPassword = masterUserPassword;
                    createClusterRequest.Port = port;

                    createClusterRequest.NodeType = nodeType;
                    createClusterRequest.ClusterType = clusterType;

                    CreateClusterResponse createClusterResponse = redshiftClient.CreateCluster(createClusterRequest);
                    Cluster newCluster = createClusterResponse.Cluster;
                    string status = newCluster.ClusterStatus;
                    Console.WriteLine("Cluster creation successful for ID {0}, initial cluster state: {1}",
                        clusterIdentifier, status);

                    while (status != "available")
                    {
                        DescribeClustersRequest describeClustersRequest = new DescribeClustersRequest
                        {
                            ClusterIdentifier = newCluster.ClusterIdentifier
                        };
                        DescribeClustersResponse describeClustersResponse =
                            redshiftClient.DescribeClusters(describeClustersRequest);
                        Cluster firstMatch = describeClustersResponse.Clusters[0];
                        status = firstMatch.ClusterStatus;
                        Console.WriteLine("Current cluster status: {0}", status);
                        Thread.Sleep(5000);
                    }
                }
                catch (AmazonRedshiftException e)
                {
                    Console.WriteLine("Cluster creation has failed.");
                    Console.WriteLine("Amazon error code: {0}",
                        string.IsNullOrEmpty(e.ErrorCode) ? "None" : e.ErrorCode);
                    Console.WriteLine("Exception message: {0}", e.Message);
                }
            }
        }

        /// <summary>
        /// Deletes a previously provisioned cluster. 
        /// A successful response from the web service indicates that the request was received correctly. 
        /// Use DescribeClusters to monitor the status of the deletion. The delete operation cannot be canceled or reverted once submitted. 
        /// For more information about managing clusters, go to Amazon Redshift Clusters in the Amazon Redshift Cluster Management Guide.
        /// </summary>
        /// <param name="clusterName">The identifier of the cluster to be deleted.</param>
        public void TerminateCluster(string clusterName)
        {
            using (IAmazonRedshift redshiftClient = GetRedShiftClient())
            {
                try
                {
                    DeleteClusterRequest deleteClusterRequest = new DeleteClusterRequest
                    {
                        ClusterIdentifier = clusterName,
                        SkipFinalClusterSnapshot = true
                    };
                    DeleteClusterResponse deleteClusterResponse = redshiftClient.DeleteCluster(deleteClusterRequest);
                    Cluster deletedCluster = deleteClusterResponse.Cluster;
                    Console.WriteLine("Cluster termination successful for cluster {0}, status after termination: {1}",
                        clusterName, deletedCluster.ClusterStatus);
                }
                catch (AmazonRedshiftException e)
                {
                    Console.WriteLine("Cluster termination has failed.");
                    Console.WriteLine("Amazon error code: {0}",
                        string.IsNullOrEmpty(e.ErrorCode) ? "None" : e.ErrorCode);
                    Console.WriteLine("Exception message: {0}", e.Message);
                }
            }
        }

        /// <summary>
        /// Copies the specified automated cluster snapshot to a new manual cluster snapshot. The source must be an automated snapshot and it must be in the available state.
        /// When you delete a cluster, Amazon Redshift deletes any automated snapshots of the cluster. 
        /// Also, when the retention period of the snapshot expires, Amazon Redshift automatically deletes it. 
        /// If you want to keep an automated snapshot for a longer period, you can make a manual copy of the snapshot. 
        /// Manual snapshots are retained until you delete them.
        /// </summary>
        /// <param name="sourceIdentifier">The identifier for the source snapshot.</param>
        /// <param name="targetIdentifier">The identifier given to the new manual snapshot.</param>
        public void CloneRedShiftClustersToSnapshot(string sourceIdentifier, string targetIdentifier)
        {
            using (IAmazonRedshift redshiftClient = GetRedShiftClient())
            {
                try
                {
                    CopyClusterSnapshotRequest copyClusterSnapshotRequest = new CopyClusterSnapshotRequest
                    {
                        SourceSnapshotIdentifier = sourceIdentifier,
                        TargetSnapshotIdentifier = targetIdentifier
                    };

                    CopyClusterSnapshotResponse copyClusterSnapshotResponse =
                        redshiftClient.CopyClusterSnapshot(copyClusterSnapshotRequest);

                    string status = copyClusterSnapshotResponse.Snapshot.Status;
                    Console.WriteLine("Cluster coped, initial cluster state: {0}", status);
                }
                catch (AmazonRedshiftException e)
                {
                    Console.WriteLine("Cluster clonning has failed.");
                    Console.WriteLine("Amazon error code: {0}", string.IsNullOrEmpty(e.ErrorCode) ? "None" : e.ErrorCode);
                    Console.WriteLine("Exception message: {0}", e.Message);
                }
            }
        }

        /// <summary>
        /// Creates a new cluster from a snapshot. By default, Amazon Redshift creates the resulting cluster with the same 
        /// configuration as the original cluster from which the snapshot was created, except that the new cluster is created 
        /// with the default cluster security and parameter groups. After Amazon Redshift creates the cluster, you can use the ModifyCluster 
        /// API to associate a different security group and different parameter group with the restored cluster. If you are using a DS node type, 
        /// you can also choose to change to another DS node type of the same size during restore.
        /// If you restore a cluster into a VPC, you must provide a cluster subnet group where you want the cluster restored.
        /// For more information about working with snapshots, go to Amazon Redshift Snapshots in the Amazon Redshift Cluster Management Guide.
        /// </summary>
        /// <param name="clusterIdentifier">The identifier of the cluster that will be created from restoring the snapshot.
        /// - Must contain from 1 to 63 alphanumeric characters or hyphens.
        /// - Alphabetic characters must be lowercase.
        /// - First character must be a letter.
        /// - Cannot end with a hyphen or contain two consecutive hyphens.
        /// - Must be unique for all clusters within an AWS account. </param>
        /// <param name="snapshotIdentifier">The name of the snapshot from which to create the new cluster. This parameter isn't case sensitive.</param>
        public void RestoreRedShiftClustersFromSnepshot(string clusterIdentifier, string snapshotIdentifier)
        {
            using (IAmazonRedshift redshiftClient = GetRedShiftClient())
            {
                try
                {
                    RestoreFromClusterSnapshotRequest restoreFromClusterSnapshotRequest = new RestoreFromClusterSnapshotRequest
                    {
                        ClusterIdentifier = clusterIdentifier,
                        SnapshotIdentifier = snapshotIdentifier
                    };

                    RestoreFromClusterSnapshotResponse restoreFromClusterSnapshotResponse =
                        redshiftClient.RestoreFromClusterSnapshot(restoreFromClusterSnapshotRequest);

                    Cluster newCluster = restoreFromClusterSnapshotResponse.Cluster;
                    string status = newCluster.ClusterStatus;
                    Console.WriteLine("Cluster restored successfuly for ID {0}, initial cluster state: {1}",
                        snapshotIdentifier,
                        status);

                    while (status != "available")
                    {
                        DescribeClustersRequest describeClustersRequest = new DescribeClustersRequest
                        {
                            ClusterIdentifier = newCluster.ClusterIdentifier
                        };
                        DescribeClustersResponse describeClustersResponse =
                            redshiftClient.DescribeClusters(describeClustersRequest);
                        Cluster firstMatch = describeClustersResponse.Clusters[0];
                        status = firstMatch.ClusterStatus;
                        Console.WriteLine("Current cluster status: {0}", status);
                        Thread.Sleep(5000);
                    }
                }
                catch (AmazonRedshiftException e)
                {
                    Console.WriteLine("Cluster restoring has failed.");
                    Console.WriteLine("Amazon error code: {0}", string.IsNullOrEmpty(e.ErrorCode) ? "None" : e.ErrorCode);
                    Console.WriteLine("Exception message: {0}", e.Message);
                }
            }
        }


    }
}
