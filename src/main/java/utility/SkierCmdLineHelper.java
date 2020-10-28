package utility;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class SkierCmdLineHelper {
    public static final String defaultServerAddr = "http://ec2-54-234-97-135.compute-1.amazonaws.com";
    public static final String defaultAPIPath = "/IntelliJ_war/skiers/liftrides";
    public static int defaultServerPort = 8080;
    public static int defaultMaxThreadCount = 4;
    public static int defaultSkierCount = 50000;
    public static int defaultSkiLiftCount = 40;
    public static int defaultSkiDay = 1;
    public static final String defaultResortName = "SilverMt";

    public static class CommandLineArgs {
        @Option(name = "-serverAddr", usage = "server ip or dns", aliases = "--serverAddr")
        public String serverAddr = defaultServerAddr;

        @Option(name = "-apiPath", usage = "api path", aliases = "--apiPath")
        public String apiPath = defaultAPIPath;

        @Option(name = "-serverPort", usage = "server port", aliases = "--serverPort")
        public int serverPort = defaultServerPort;

        @Option(name = "-maxThreadCount", usage = "max thread to use", aliases = "--maxThreadCount")
        public int maxThreadCount = defaultMaxThreadCount;

        @Option(name = "-skierCount", usage = "number of skiers", aliases = "--skierCount")
        public int skierCount = defaultSkierCount;

        @Option(name = "-skiLiftCount", usage = "number of ski lifts", aliases = "--skiLiftCount")
        public int skiLiftCount = defaultSkiLiftCount;

        @Option(name = "-skiDay", usage = "day of ski", aliases = "--skiDay")
        public int skiDay = defaultSkiDay;

        @Option(name = "-resortID", usage = "name of resort", aliases = "--resortID")
        public String resortName = defaultResortName;
    }

    public static CommandLineArgs parseCommandLineArgs(String[] argv) throws CmdLineException {
        final CommandLineArgs args = new CommandLineArgs();

        final CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withOptionValueDelimiter("="));
        parser.parseArgument(argv);

        return args;
    }
}
