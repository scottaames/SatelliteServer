package appserver.satellite;

import appserver.job.Job;
import appserver.comm.ConnectivityInfo;
import appserver.job.UnknownToolException;
import appserver.comm.Message;
import static appserver.comm.MessageTypes.JOB_REQUEST;
import static appserver.comm.MessageTypes.REGISTER_SATELLITE;
import appserver.job.Tool;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;
import utils.PropertyHandler;

/**
 * Class [Satellite] Instances of this class represent computing nodes that execute jobs by
 * calling the callback method of tool a implementation, loading the tool's code dynamically over a network
 * or locally from the cache, if a tool got executed before.
 *
 * @author Dr.-Ing. Wolf-Dieter Otte
 */
public class Satellite extends Thread {

    private ConnectivityInfo satelliteInfo = new ConnectivityInfo();
    private ConnectivityInfo serverInfo = new ConnectivityInfo();
    private HTTPClassLoader classLoader;
    private Hashtable<String, Tool> toolsCache;

    public Satellite(String satellitePropertiesFile, String classLoaderPropertiesFile, String serverPropertiesFile) {

        // read this satellite's properties and populate satelliteInfo object,
        // which later on will be sent to the server
        // --------------------------
        try {
            PropertyHandler satelliteConfigFile = new PropertyHandler(satellitePropertiesFile);
            satelliteInfo.setName(satelliteConfigFile.getProperty("NAME"));
            satelliteInfo.setPort(Integer.parseInt(satelliteConfigFile.getProperty("PORT")));
        } catch (IOException e) {
            System.err.println("No Satellite config file found, bailing out ...");
            System.exit(1);
        }

        // read properties of the application server and populate serverInfo object
        // other than satellites, the as doesn't have a human-readable name, so leave it out
        // --------------------------------
        try {
            PropertyHandler serverConfigFile = new PropertyHandler(serverPropertiesFile);
            serverInfo.setPort(Integer.parseInt(serverConfigFile.getProperty("PORT")));
            serverInfo.setHost(serverConfigFile.getProperty("HOST"));
        } catch (IOException e) {
            System.err.println("No Server config file found, bailing out ...");
            System.exit(1);
        }

        // read properties of the code server and create class loader
        // -------------------
        try {
            PropertyHandler classLoaderConfigFile = new PropertyHandler(classLoaderPropertiesFile);
            classLoader = new HTTPClassLoader(classLoaderConfigFile.getProperty("HOST"), Integer.parseInt(classLoaderConfigFile.getProperty("PORT")));
        } catch (IOException e) {
            System.err.println("No HTTPClassLoader config file found, bailing out ...");
            System.exit(1);
        }

        // create tools cache
        // -------------------
        toolsCache = new Hashtable();

    }

    @Override
    public void run() {

        // ** IGNORE **
        // register this satellite with the SatelliteManager on the server
        // ---------------------------------------------------------------
        // ...


        // create server socket
        // ---------------------------------------------------------------
        try {
            ServerSocket serverSocket = new ServerSocket(satelliteInfo.getPort());
            System.out.println("[Satellite.run] ServerSocket created");
            // start taking job requests in a server loop
            // ---------------------------------------------------------------
            while (true) {
                new SatelliteThread(serverSocket.accept(), this).run();
            }
        } catch (IOException e) {
            System.err.println("[Satellite.run] Error creating ServerSocket");
        }
    }


    // inner helper class that is instanciated in above server loop and processes single job requests
    private class SatelliteThread extends Thread {

        Satellite satellite = null;
        Socket jobRequest = null;
        ObjectInputStream readFromNet = null;
        ObjectOutputStream writeToNet = null;
        Message message = null;

        SatelliteThread(Socket jobRequest, Satellite satellite) {
            this.jobRequest = jobRequest;
            this.satellite = satellite;
        }

        @Override
        public void run() {
            // setting up object streams
            try {
                readFromNet = new ObjectInputStream(jobRequest.getInputStream());
                writeToNet = new ObjectOutputStream(jobRequest.getOutputStream());

                // reading message
                message = (Message) readFromNet.readObject();
                switch (message.getType()) {
                    case JOB_REQUEST:
                        try {
                            Job job = (Job) message.getContent();
                            String toolClassString = job.getToolName();
                            Tool tool = getToolObject(toolClassString);
                            Object result = tool.go(job.getParameters());
                            writeToNet.writeObject(result);
                        } catch ( UnknownToolException | InstantiationException | IllegalAccessException| ClassNotFoundException e) {
                            System.err.println("[SatelliteThread.run] Error processing job request.");
                            e.printStackTrace();
                        }
                        break;

                    default:
                        System.err.println("[SatelliteThread.run] Warning: Message type not implemented");
                }
            } catch (IOException | ClassNotFoundException e) {
                System.out.println("[Satellite.SatelliteThread] Error creating reading message from I/O Stream");
            }

        }
    }

    /**
     * Aux method to get a tool object, given the fully qualified class string
     * If the tool has been used before, it is returned immediately out of the cache,
     * otherwise it is loaded dynamically
     */
    public Tool getToolObject(String toolClassString) throws UnknownToolException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        Tool toolObject = toolsCache.get(toolClassString);

        if (toolObject == null) {
            System.out.println("\nTool's Class: " + toolClassString);
            if (toolClassString == null) {
                throw new UnknownToolException();
            }
            Class toolClass = classLoader.loadClass(toolClassString);
            toolObject = (Tool) toolClass.newInstance();
            toolsCache.put(toolClassString, toolObject);
        } else {
            System.out.println("Tool Class: \"" + toolClassString + "\" already in Cache.");
        }

        return toolObject;
    }

    public static void main(String[] args) {
        // start the satellite
        Satellite satellite = new Satellite(args[0], args[1], args[2]);
        satellite.run();
    }
}
