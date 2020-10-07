import java.io.*;
import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;

@WebServlet("/hello")
public class HelloWorldServlet extends HttpServlet {
    private String msg;

    public void init() throws ServletException {
        // Initialization
        msg = "Hello World";
    }

    // handle a GET request
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        // Set response content type to text
        response.setContentType("text/html");

        PrintWriter out = response.getWriter();

        // sleep for 1000ms. You can vary this value for different tests
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            out.println("Encountered error during sleep: " + e);
        }

        // Send the response
        out.println("<h1>" + msg + "</h1>");
    }

    public void destroy() {
        // nothing to do here
    }
}