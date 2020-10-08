package utility;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class SkierCmdLineHelper {
    public static class CommandLineArgs {
        @Option(name = "-serverAddr", usage = "server ip or dns", aliases = "--serverAddr")
        public String serverAddr = "http://ec2-54-92-222-44.compute-1.amazonaws.com";

        @Option(name = "-apiPath", usage = "api path", aliases = "--apiPath")
        public String apiPath = "/IntelliJ_war/skiers/liftrides";

        @Option(name = "-serverPort", usage = "server port", aliases = "--serverPort")
        public int serverPort = 8080;

        @Option(name = "-maxThreadCount", usage = "max thread to use", aliases = "--maxThreadCount")
        public int maxThreadCount = 256;

        @Option(name = "-skierCount", usage = "number of skiers", aliases = "--skierCount")
        public int skierCount = 50000;

        @Option(name = "-skiLiftCount", usage = "number of ski lifts", aliases = "--skiLiftCount")
        public int skiLiftCount = 40;

        @Option(name = "-skiDay", usage = "day of ski", aliases = "--skiDay")
        public int skiDay = 1;

        @Option(name = "-resortName", usage = "name of resort", aliases = "--resortName")
        public String resortName = "SilverMt";
    }

    public static CommandLineArgs parseCommandLineArgs(String[] argv) throws CmdLineException {
        final CommandLineArgs args = new CommandLineArgs();

        final CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withOptionValueDelimiter("="));
        parser.parseArgument(argv);

        return args;
    }
}
