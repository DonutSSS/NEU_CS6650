package clientAssignment1Part1;

import base.SkierClientBase;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.kohsuke.args4j.CmdLineException;
import utility.SkierCmdLineHelper;

import java.util.concurrent.ExecutionException;

public class SkierClientAIP1 extends SkierClientBase {
    public SkierClientAIP1(final String serverIp,
                           final String apiPath,
                           int serverPort,
                           int maxThreadCount,
                           int skierCount,
                           int skiLiftCount,
                           int skiDayNum,
                           final String resortName) {
        super(serverIp,
                apiPath,
                serverPort,
                maxThreadCount,
                skierCount,
                skiLiftCount,
                skiDayNum,
                resortName);
    }

    public static void main(String[] args) throws JsonProcessingException, InterruptedException, ExecutionException, CmdLineException {
        SkierCmdLineHelper.CommandLineArgs parsedArgs = SkierCmdLineHelper.parseCommandLineArgs(args);

        SkierClientAIP1 client = new SkierClientAIP1(parsedArgs.serverAddr,
                parsedArgs.apiPath,
                parsedArgs.serverPort,
                parsedArgs.maxThreadCount,
                parsedArgs.skierCount,
                parsedArgs.skiLiftCount,
                parsedArgs.skiDay,
                parsedArgs.resortName);

        client.startLoadSimulation();
    }
}
