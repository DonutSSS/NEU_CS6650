package server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;

public class IkkyoneServlet extends javax.servlet.http.HttpServlet {
    private final static ObjectMapper mapper = new ObjectMapper();

    protected void doPost(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response) throws IOException {
        // Prepare response data.
        response.setContentType("text/plain");

        if (isValidRequest(request, response, false)) {
            // Check POST body.
            JsonNode[] nodeHolder = new JsonNode[1];
            if (isBodyValidOnPost(request, response, nodeHolder)) {
                response.setStatus(HttpServletResponse.SC_OK);
                response.getWriter().printf("It works! Post request url: %s, body: %s\n",
                        request.getPathInfo(),
                        nodeHolder[0].toPrettyString());
            }
        }
    }

    protected void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response) throws IOException {
        // Prepare response data.
        response.setContentType("text/plain");

        if (isValidRequest(request, response, true)) {
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().printf("It works! Get request url: %s\n",
                    request.getPathInfo());
        }
    }

    private boolean isValidRequest(javax.servlet.http.HttpServletRequest request,
                                   javax.servlet.http.HttpServletResponse response,
                                   boolean isGETRequest) throws IOException {
        // Get URL Path from request.
        String urlPath = request.getPathInfo();

        // Check if URL Path exists.
        if (!hasPath(urlPath)) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("Missing path in url.");

            return false;
        }

        // Parse URL Path.
        String[] urlParts = urlPath.split("/");

        // Check if URL Path is valid.
        if (!hasValidPath(urlParts, isGETRequest)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().printf("Unsupported path in url: %s | %s\n", urlPath, Arrays.toString(urlParts));

            return false;
        }

        return true;
    }

    private boolean hasPath(String urlPath) {
        return urlPath != null && urlPath.length() > 0;
    }

    // Based on the API spec in: https://app.swaggerhub.com/apis/cloud-perf/SkiDataAPI/1.13#
    private boolean hasValidPath(String[] urlPath,
                                 boolean isGetRequest) {
        // Validate the request url path according to the API spec
        // urlPath  = "/1/seasons/2019/day/1/skier/123"
        // urlParts = [, 1, seasons, 2019, day, 1, skier, 123]
        if (isGetRequest) {
            return isValidGETPath(urlPath);
        } else {
            return isValidPOSTPath(urlPath);
        }
    }

    private boolean isValidGETPath(String[] urlPath) {
        if (urlPath.length == 4) {
            if (urlPath[1].equals("resort") &&
                    urlPath[2].equals("day") &&
                    urlPath[3].equals("top10vert")) {
                return true;
            } else if (urlPath[1].equals("skiers") &&
                    !urlPath[2].isEmpty() &&
                    urlPath[3].equals("vertical")) {
                return true;
            } else {
                return false;
            }
        } else if (urlPath.length == 7) {
            return urlPath[1].equals("skiers") &&
                    !urlPath[2].isEmpty() &&
                    urlPath[3].equals("days") &&
                    !urlPath[4].isEmpty() &&
                    urlPath[5].equals("skiers") &&
                    !urlPath[6].isEmpty();
        } else {
            return false;
        }
    }

    private boolean isValidPOSTPath(String[] urlPath) {
        if (urlPath.length == 3) {
            return urlPath[1].equals("skiers") &&
                    urlPath[2].equals("liftrides");
        }

        return false;
    }

    // Check if the string can be converted into a Json object, and retrieve that object for further processing.
    private boolean isBodyValidOnPost(javax.servlet.http.HttpServletRequest request,
                                      javax.servlet.http.HttpServletResponse response,
                                      JsonNode[] outputNodeHolder) throws IOException {
        String requestBody = getBodyStrOnPost(request.getReader());

        try {
            JsonNode node = mapper.readTree(requestBody);
            outputNodeHolder[0] = node;

            return true;
        } catch (IOException e) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.getWriter().write("Invalid body string in POST request.");

            return false;
        }
    }

    private String getBodyStrOnPost(BufferedReader buffIn) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;

        while((line = buffIn.readLine()) != null) {
            sb.append(line);
        }

        return sb.toString();
    }
}
