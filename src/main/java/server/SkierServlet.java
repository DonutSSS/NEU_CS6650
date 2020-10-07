package server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;

public class SkierServlet extends javax.servlet.http.HttpServlet {
    private final static ObjectMapper MAPPER = new ObjectMapper();

    protected void doPost(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response) throws javax.servlet.ServletException, IOException {
        // Prepare response data.
        response.setContentType("text/plain");

        // Get URL from request.
        String urlPath = request.getPathInfo();

        // Check if URL exists.
        if (!hasUrl(urlPath, response)) {
            return;
        }

        // Check and process URL if it is valid.
        String requestBody = getBodyStrOnPost(request.getReader());
        JsonNode[] nodeHolder = new JsonNode[1];

        if (!isBodyValidOnPost(requestBody, nodeHolder)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write("It works! Post request body: " + nodeHolder[0].toPrettyString());
        }
    }

    protected void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response) throws javax.servlet.ServletException, IOException {
        // Prepare response data.
        response.setContentType("text/plain");

        // Get URL from request.
        String urlPath = request.getPathInfo();

        // Check if URL exists.
        if (!hasUrl(urlPath, response)) {
            return;
        }

        // Check and process URL if it is valid.
        String[] urlParts = urlPath.split("/");
        if (!isUrlValid(urlParts)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } else {
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().write("It works! Get request url params: " + Arrays.toString(urlParts));
        }
    }

    private boolean hasUrl(String urlPath,
                           javax.servlet.http.HttpServletResponse response) throws IOException {
        if (urlPath == null || urlPath.length() == 0) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("Missing parameters in url.");

            return false;
        }

        return true;
    }

    private boolean isUrlValid(String[] urlPath) {
        // Validate the request url path according to the API spec
        // urlPath  = "/1/seasons/2019/day/1/skier/123"
        // urlParts = [, 1, seasons, 2019, day, 1, skier, 123]
        return urlPath.length > 0;
    }

    private String getBodyStrOnPost(BufferedReader buffIn) throws IOException {
        StringBuilder sb = new StringBuilder();
        String line;

        while((line = buffIn.readLine()) != null) {
            sb.append(line);
        }

        return sb.toString();
    }

    // Check if the string can be converted into a Json object, and retrieve that object for further processing.
    private boolean isBodyValidOnPost(String body, JsonNode[] outputNodeHolder) {
        try {
            JsonNode node = MAPPER.readTree(body);
            outputNodeHolder[0] = node;

            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
