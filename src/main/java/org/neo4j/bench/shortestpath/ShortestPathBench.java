package org.neo4j.bench.shortestpath;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.path.Dijkstra;
import org.neo4j.graphalgo.impl.path.DijkstraBidirectional;
import org.neo4j.graphalgo.impl.path.ShortestPath;
import org.neo4j.graphalgo.impl.util.PathInterest;
import org.neo4j.graphalgo.impl.util.PathInterestFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.util.MutableInteger;
import org.neo4j.kernel.impl.util.NoneStrictMath;

public class ShortestPathBench
{
    private static String PATHS_SINGLE = "single";
    private static String PATHS_ALL = "all";
    private static String PATHS_INCREASING = "increasing";
    private static String TEST_PERFORMANCE = "bench";
    private static String TEST_CORRECTNESS = "corr";
    private static String DIRECTION_BOTH = "both";
    private static String DIRECTION_OUT = "out";
    private static double epsilon = NoneStrictMath.EPSILON;

    public static void main( String[] args ) throws IOException
    {
        String errMsg = String.format( "Expected 3 parameters, found %s. Parameters should be <%s|%s|%s> <%s|%s> <%s|%s>\n",
                args.length, PATHS_SINGLE, PATHS_ALL, PATHS_INCREASING,
                             DIRECTION_BOTH, DIRECTION_OUT,
                             TEST_PERFORMANCE, TEST_CORRECTNESS);

        if ( args.length != 3 )
        {
            System.out.println( errMsg );
            return;
        }

        if ( args[0].equals( PATHS_ALL ) == false &&
             args[0].equals( PATHS_SINGLE ) == false &&
             args[0].equals( PATHS_INCREASING ) == false )
        {
            System.out.println( String.format( "Unexpected value for parameter 0: %s\n%s", args[0], errMsg ) );
            return;
        }

        if ( args[1].equals( DIRECTION_BOTH ) == false && args[1].equals( DIRECTION_OUT ) == false )
        {
            System.out.println( String.format( "Unexpected value for parameter 1: %s\n%s", args[1], errMsg ) );
            return;
        }

        if ( args[2].equals( TEST_PERFORMANCE ) == false && args[2].equals( TEST_CORRECTNESS ) == false )
        {
            System.out.println( String.format( "Unexpected value for parameter 2: %s\n%s", args[1], errMsg ) );
            return;
        }

        GraphDatabaseService db = null;
        try
        {
            db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( Config.DB_DIR ).setConfig(
                    Config.NEO4J_CONFIG ).newGraphDatabase();

            System.out.println( "Node Count = " + GraphUtils.nodeCount( db, 1000 ) );
            System.out.println( "Node Property Count = " + GraphUtils.nodePropertyCount( db, 1000 ) );
            System.out.println( "Relationship Count = " + GraphUtils.relationshipCount( db, 1000 ) );
            System.out.println( "Relationship Property Count = " + GraphUtils.relationshipPropertyCount( db, 1000 ) );

            int runCount = 100;
            List<Pair<Node>> startAndEndNodes = loadStartAndEndNodes( db, runCount );
            Direction direction = ( args[1].equals( DIRECTION_BOTH ) ) ? Direction.BOTH : Direction.OUTGOING;
            System.out.println( "Paths =\t\t" + args[0] );
            System.out.println( "Direction =\t" + direction );

            PathExpander expander = PathExpanders.forDirection( direction );
            MutableInteger expandCount = new MutableInteger( 0 );
            if ( args[2].equals( TEST_PERFORMANCE ) )
            {
                expander = PathExpanders.countingWrapper( expander, expandCount );
            }

            CostEvaluator<Double> evaluator = CommonEvaluators.doubleCostEvaluator( "weight" );
            int maxDepth = Integer.MAX_VALUE;
            PathFinder<?> shortestPath = GraphAlgoFactory.shortestPath( expander, maxDepth );

            CostEvaluator<Double> constantEvaluator = new CostEvaluator<Double>()
            {
                @Override
                public Double getCost( Relationship relationship, Direction direction )
                {
                    return 1D;
                }
            };
            PathFinder<? extends Path> unweightedDijkstra = new Dijkstra( expander, constantEvaluator );
            PathFinder<? extends Path> unweightedDijkstraBidirectional = new DijkstraBidirectional(
                    expander, constantEvaluator );

            PathInterest<Double> interest = PathInterestFactory.allShortest( epsilon );
            PathFinder<? extends Path> weightedDijkstra = new Dijkstra( expander, evaluator, epsilon,
                    interest );

            PathFinder<? extends Path> weightedDijkstraBidirectional = new DijkstraBidirectional( expander, evaluator,
                    epsilon );

            int pathsToFind = 1;
            PathFinder<? extends Path> increasingDijkstra = new Dijkstra( expander, evaluator, epsilon,
                    PathInterestFactory.numberOfShortest( epsilon, pathsToFind ) );

            warmUp( startAndEndNodes );
            if ( args[0].equals( PATHS_SINGLE ) )
            {
                if (args[2].equals( TEST_PERFORMANCE ) )
                {

                    System.out.println( "- Shortest Path -\n" + runFindSinglePath( shortestPath, startAndEndNodes, expandCount ) );
                    System.out.println( "- Unweighted Dijkstra -\n" + runFindSinglePath( unweightedDijkstra, startAndEndNodes, expandCount ) );
                    System.out.println( "- Unweighted Bidirectional Dijkstra -\n" + runFindSinglePath( unweightedDijkstraBidirectional, startAndEndNodes, expandCount ) );
                    System.out.println( "- Weighted Dijkstra -\n" + runFindSinglePath( weightedDijkstra, startAndEndNodes, expandCount ) );
                    System.out.println( "- Weighted Bidirectional Dijkstra -\n" + runFindSinglePath(
                            weightedDijkstraBidirectional, startAndEndNodes, expandCount ) );
                }
                else if ( args[2].equals( TEST_CORRECTNESS ) )
                {
                    testEquivalenceSingle( weightedDijkstra, weightedDijkstraBidirectional, startAndEndNodes );
                }
            }
            else if ( args[0].equals( PATHS_ALL ) )
            {
                if ( args[2].equals( TEST_PERFORMANCE ) )
                {
                    System.out.println( "- Shortest Path -\n" + runFindAllPaths( shortestPath, startAndEndNodes, expandCount ) );
                    System.out.println( "- Unweighted Dijkstra -\n" + runFindAllPaths( unweightedDijkstra, startAndEndNodes, expandCount ) );
                    System.out.println( "- Unweighted Bidirectional Dijkstra -\n" + runFindAllPaths( unweightedDijkstraBidirectional, startAndEndNodes, expandCount ) );
                    System.out.println(
                            "- Weighted Dijkstra -\n" + runFindAllPaths( weightedDijkstra, startAndEndNodes, expandCount ) );
                    System.out.println( "- Weighted Bidirectional Dijkstra -\n" + runFindAllPaths(
                            weightedDijkstraBidirectional, startAndEndNodes, expandCount ) );
                }
                else if ( args[2].equals( TEST_CORRECTNESS ) )
                {
                    testEquivalenceAll( weightedDijkstra, weightedDijkstraBidirectional, startAndEndNodes, true, true );
                }
            }
            else if (args[0].equals( PATHS_INCREASING ) )
            {
                testCorrectnessIncreasing( increasingDijkstra, startAndEndNodes, pathsToFind );
            }
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    public static void warmUp( List<Pair<Node>> startAndEndNodes )
    {
        PathFinder finder = new ShortestPath( Integer.MAX_VALUE, PathExpanders.allTypesAndDirections() );
        for ( Pair<Node> entry : startAndEndNodes )
        {
            Node start = entry.first;
            Node end = entry.second;

            Iterator allPaths = finder.findAllPaths( start, end ).iterator();

            while ( allPaths.hasNext() )
            {
                allPaths.next();
            }
        }
    }

    public static String runFindSinglePath( PathFinder<? extends Path> pathFinder, List<Pair<Node>> startAndEndNodes,
            MutableInteger expandCount )
    {
        RelationshipStore.count = 0;
        expandCount.value = 0;

        Histogram timeHistogram = new Histogram( TimeUnit.MILLISECONDS.convert( 10, TimeUnit.MINUTES ), 5 );
        Histogram pathLengthHistogram = new Histogram( 10000, 5 );
        long longestRuntime = Long.MIN_VALUE;
        long longestRunTimeStartNodeId = -1;
        long longestRunTimeEndNodeId = -1;
        long longestRunTimePathLength = -1;

        int runCount = 0;
        for ( Pair<Node> startAndEndNode : startAndEndNodes )
        {
            long startTime = System.currentTimeMillis();
            Path path = pathFinder.findSinglePath( startAndEndNode.getFirst(), startAndEndNode.getSecond() );
            long runTime = System.currentTimeMillis() - startTime;
            if ( path == null ) continue;
            timeHistogram.recordValue( runTime );
            pathLengthHistogram.recordValue( path.length() );
            if ( runTime > longestRuntime )
            {
                longestRuntime = runTime;
                longestRunTimeStartNodeId = startAndEndNode.getFirst().getId();
                longestRunTimeEndNodeId = startAndEndNode.getSecond().getId();
                longestRunTimePathLength = path.length();
            }
            runCount++;
            if ( runCount % 100 == 0 ) System.out.println( "run: " + runCount );
        }
        String longestRunTimeString = String.format( "\tLongest Run\t\t : Time[%s(ms)] Start[%s] End[%s] Length[%s]\n",
                longestRuntime, longestRunTimeStartNodeId, longestRunTimeEndNodeId, longestRunTimePathLength );
        return histogramString( timeHistogram, "Run Time (ms)" ) + histogramString( pathLengthHistogram, "Path Length" )
               + longestRunTimeString + "fillRecord = " + RelationshipStore.count + "\n" +
                "expand count = " + expandCount.value + "\n";
    }

    public static String runFindAllPaths( PathFinder<? extends Path> pathFinder, List<Pair<Node>> startAndEndNodes,
            MutableInteger expandCount )
    {
        RelationshipStore.count = 0;
        expandCount.value = 0;

        Histogram timeHistogram = new Histogram( TimeUnit.MILLISECONDS.convert( 10, TimeUnit.MINUTES ), 5 );
        Histogram pathLengthHistogram = new Histogram( 10000, 5 );
        Histogram pathCountHistogram = new Histogram( 10000, 5 );
        long longestRuntime = Long.MIN_VALUE;
        long longestRunTimeStartNodeId = -1;
        long longestRunTimeEndNodeId = -1;

        int runCount = 0;
        for ( Pair<Node> startAndEndNode : startAndEndNodes )
        {
            long startTime = System.currentTimeMillis();
            Iterator<? extends Path> paths = pathFinder.findAllPaths( startAndEndNode.getFirst(),
                    startAndEndNode.getSecond() ).iterator();
            int pathCount = 0;
            while ( paths.hasNext() )
            {
                pathLengthHistogram.recordValue( paths.next().length() );
                pathCount++;
            }
            long runTime = System.currentTimeMillis() - startTime;
            pathCountHistogram.recordValue( pathCount );
            timeHistogram.recordValue( runTime );
            if ( runTime > longestRuntime )
            {
                longestRuntime = runTime;
                longestRunTimeStartNodeId = startAndEndNode.getFirst().getId();
                longestRunTimeEndNodeId = startAndEndNode.getSecond().getId();
            }
            runCount++;
            if ( runCount % 100 == 0 )
                System.out.println( "run: " + runCount );
        }
        String longestRunTimeString = String.format( "\tLongest Run\t\t : Time[%s(ms)] Start[%s] End[%s]\n",
                longestRuntime, longestRunTimeStartNodeId, longestRunTimeEndNodeId );
        return histogramString( timeHistogram, "Run Time (ms)" ) + histogramString( pathLengthHistogram, "Path Length" )
               + histogramString( pathCountHistogram, "Discovered Path Count" ) + longestRunTimeString +
               "fillRecord = " + RelationshipStore.count + "\n" +
               "expand count = " + expandCount.value + "\n";
    }

    public static String histogramString( Histogram histogram, String name )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "\t" ).append( name ).append( "\n" );
        sb.append( "\t\tCOUNT\t\t : " ).append( histogram.getTotalCount() ).append( "\n" );
        sb.append( "\t\tMIN\t\t : " ).append( histogram.getMinValue() ).append( "\n" );
        sb.append( "\t\tMAX\t\t : " ).append( histogram.getMaxValue() ).append( "\n" );
        sb.append( "\t\t50th PERCENTILE\t : " ).append( histogram.getValueAtPercentile( 50 ) ).append(
                "\n" );
        sb.append( "\t\t90th PERCENTILE\t : " ).append( histogram.getValueAtPercentile( 90 ) ).append(
                "\n" );
        sb.append( "\t\t95th PERCENTILE\t : " ).append( histogram.getValueAtPercentile( 95 ) ).append(
                "\n" );
        sb.append( "\t\t99th PERCENTILE\t : " ).append( histogram.getValueAtPercentile( 99 ) ).append(
                "\n" );
        sb.append( "\t\tMEAN\t\t : " ).append( histogram.getMean() ).append( "\n" );
        return sb.toString();
    }

    public static List<Pair<Node>> loadStartAndEndNodes( GraphDatabaseService db, int maxCount )
    {
        int count = 0;
        List<Pair<Node>> startAndEndNodes = new ArrayList<Pair<Node>>();
        Transaction tx = db.beginTx();
        try
        {
            CsvFileReader reader = new CsvFileReader( new File( Config.PATH_START_END_ID_FILE ), "," );
            // Skip Files Headers
            if ( reader.hasNext() )
            {
                reader.next();
            }

            while ( reader.hasNext() )
            {
                String[] startAndEndNode = reader.next();
                long startNodeId = Long.parseLong( startAndEndNode[0] );
                long endNodeId = Long.parseLong( startAndEndNode[1] );
                Node startNode = db.getNodeById( startNodeId );
                Node endNode = db.getNodeById( endNodeId );
                startAndEndNodes.add( new Pair( startNode, endNode ) );
                count++;
                if ( count >= maxCount ) break;
            }
            tx.success();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e.getCause() );
        }
        finally
        {
            tx.finish();
        }

        return startAndEndNodes;
    }

    static class Pair<T>
    {
        private final T first;
        private final T second;

        private Pair( T first, T second )
        {
            this.first = first;
            this.second = second;
        }

        public T getFirst()
        {
            return first;
        }

        public T getSecond()
        {
            return second;
        }

        @Override
        public String toString()
        {
            return "Pair[" + first + "," + second + "]";
        }
    }

    public static void testEquivalenceSingle( PathFinder<? extends Path> dijkstra,
            PathFinder<? extends Path> bidirectional,
            List<Pair<Node>> startAndEndNodes )
    {
        int count = 0;
        for ( Pair<Node> pair : startAndEndNodes )
        {
            Node start = pair.getFirst();
            Node end = pair.getSecond();
            WeightedPath pathDijkstra = (WeightedPath)dijkstra.findSinglePath( start, end );

            WeightedPath pathBidirectional = (WeightedPath)bidirectional.findSinglePath( start, end );

            System.out.printf( "\n(%d) -> (%d)\n", start.getId(), end.getId() );
            System.out.println( "    " + pathDijkstra.toString() );
            System.out.println( "    " + pathBidirectional.toString() );
            if ( NoneStrictMath.compare( pathDijkstra.weight(), pathBidirectional.weight(), NoneStrictMath.EPSILON ) != 0 )
            {
                System.out.printf( "(%d) -> (%d) dijk: %f bid: %f\n",
                        start.getId(), end.getId(), pathDijkstra.weight(), pathBidirectional.weight() );
                System.out.println( "    dijk: " + pathDijkstra );
                System.out.println( "     bid: " + pathBidirectional );
            }
            count++;
//            System.out.println( count );
        }
    }

    public static void testEquivalenceAll( PathFinder<? extends Path> dijkstra,
            PathFinder<? extends Path> bidirectional,
            List<Pair<Node>> startAndEndNodes, boolean printNotFound, boolean printExtra )
    {
        Set<String> shouldFind = new HashSet<String>();
        Set<String> dijkstraFind = new HashSet<String>();
        Set<String> bidirectionalFind = new HashSet<String>();
        Set<String> extraFind = new HashSet<String>();
        int count = 0;
        for ( Pair<Node> pair : startAndEndNodes )
        {
            double dijkstraWeight = -1;
            double bidirecitonalWeight = -1;
            Node start = pair.getFirst();
            Node end = pair.getSecond();
            Iterator<? extends Path> paths = dijkstra.findAllPaths( start, end ).iterator();
            while ( paths.hasNext() )
            {
                WeightedPath path = ( WeightedPath ) paths.next();
                dijkstraWeight = path.weight();
                String string = path.toString();
                shouldFind.add( string );
                dijkstraFind.add( string );
            }
            paths = bidirectional.findAllPaths( start, end ).iterator();
            while ( paths.hasNext() )
            {
                WeightedPath path = ( WeightedPath ) paths.next();
                bidirecitonalWeight = path.weight();
                String string = path.toString();
                bidirectionalFind.add( string );
                if ( !shouldFind.remove( string ) )
                {
                    extraFind.add( string );
                }
            }
            if ( !shouldFind.isEmpty() || !extraFind.isEmpty() )
            {
                System.out.printf( "(%d) -> (%d)\n", start.getId(), end.getId() );
                System.out.printf( "Dijkstra found %d paths of weight %f.\n", dijkstraFind.size(), dijkstraWeight );
                System.out.printf( "Bidirectional found %d paths of weight %f.\n", bidirectionalFind.size(), bidirecitonalWeight );
            }
            if ( !shouldFind.isEmpty() && printNotFound )
            {
                System.out.println( "    Did not find " );
                for ( String path : shouldFind )
                {
                    System.out.println( "        " + path );
                }
            }
            if ( !extraFind.isEmpty() && printExtra )
            {
                System.out.println( "    Found extra " );
                for ( String path : extraFind )
                {
                    System.out.println( "        " + path );
                }
            }
            shouldFind.clear();
            extraFind.clear();
            dijkstraFind.clear();
            bidirectionalFind.clear();

            count++;
//            if ( count % 1 == 0 )
//            {
                System.out.println( "count = " + count );
//            }
        }
    }


    private static void testCorrectnessIncreasing( PathFinder<? extends Path> dijkstra,
            List<Pair<Node>> startAndEndNodes, int pathsToFind )
    {
        List<String> dijkstraFind = new ArrayList<>();
        int count = 0;
        for ( Pair<Node> pair : startAndEndNodes )
        {
            double dijkstraWeight = -1;
            double lastWeight = 0;
            boolean outOfOrder = false;
            int pathCount = 0;
            Node start = pair.getFirst();
            Node end = pair.getSecond();
            System.out.printf( "(%d) -> (%d)\n", start.getId(), end.getId() );
            Iterator<? extends Path> paths = dijkstra.findAllPaths( start, end ).iterator();
            while ( paths.hasNext() )
            {
                WeightedPath path = ( WeightedPath ) paths.next();
                if ( NoneStrictMath.compare( lastWeight, path.weight(), epsilon ) > 0 ) {
                    outOfOrder = true;
                }
                lastWeight = path.weight();
                if ( dijkstraWeight == -1 ) dijkstraWeight = lastWeight;
                String string = path.toString();
                dijkstraFind.add( string );
                pathCount++;
            }

            if ( outOfOrder || pathCount != pathsToFind)
            {
                if ( outOfOrder ) System.out.print( "Out of order" );
                if ( pathCount != pathsToFind ) System.out.println( "Wrong number of paths" );
                for ( String path : dijkstraFind )
                {
                    System.out.println( "    " + path );
                }
            }
            dijkstraFind.clear();

            count++;
            if ( count % 100 == 0 )
            {
                System.out.println( "count = " + count );
            }
        }
    }
}
