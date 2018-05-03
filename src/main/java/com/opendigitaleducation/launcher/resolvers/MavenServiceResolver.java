package com.opendigitaleducation.launcher.resolvers;

import com.opendigitaleducation.launcher.utils.DefaultAsyncResult;
import com.opendigitaleducation.launcher.utils.JsonUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.ProxyOptions;
import io.vertx.core.net.ProxyType;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static com.opendigitaleducation.launcher.utils.DefaultAsyncResult.handleAsyncError;

public class MavenServiceResolver extends AbstactServiceResolver {

    public static final String MAVEN_METADATA_XML = "maven-metadata.xml";
    private JsonArray releasesRepositories;
    private JsonArray snapshotsRepositories;
    private List<HttpClient> releasesClients;
    private List<HttpClient> snapshotsClients;

    @Override
    public void init(Vertx vertx, String servicesPath) {
        super.init(vertx, servicesPath);
        JsonObject repositories = null;
        try {
            String r = System.getenv("MAVEN_REPOSITORIES");
            if (r != null) {
                repositories = new JsonObject(r);
            }
        } catch (Exception e) {
            log.error("Invalid env variable MAVEN_REPOSITORIES.", e);
        }
        if (repositories == null) {
            repositories = JsonUtil.loadFromResource("maven-repositories.json");
        }
        releasesRepositories = repositories.getJsonArray("releases", new JsonArray());
        snapshotsRepositories = repositories.getJsonArray("snapshots", new JsonArray());
        releasesClients = new ArrayList<>();
        snapshotsClients = new ArrayList<>();
    }

    @Override
    public void resolve(String identifier, Handler<AsyncResult<String>> handler) {
        final String [] id = identifier.split("~");
        if (id.length != 3) {
            handleAsyncError(new NotFoundServiceException("invalid.identifier"), handler);
            return;
        }
        String path = id[0].replaceAll("\\.", "/") + "/" + id[1] + "/" + id[2] + "/";
        if (id[2].endsWith("-SNAPSHOT")) {
            path += MAVEN_METADATA_XML;
            downloadService(0, identifier, path, snapshotsRepositories, snapshotsClients, handler);
        } else {
            path += id[1] + "-" + id[2] + "-fat.jar";
            downloadService(0, identifier, path, releasesRepositories, releasesClients, handler);
        }
    }

    private void downloadService(int index, String identifier, String path,
            JsonArray repositories, List<HttpClient> clients, Handler<AsyncResult<String>> handler) {
        if (index >= repositories.size()) {
            handleAsyncError(new NotFoundServiceException(), handler);
            return;
        }
        final String repository = repositories.getJsonObject(index).getString("uri");
        HttpClient client;
        if (index >= clients.size()) {
            try {
                client = createClient(repository, clients);
            } catch (URISyntaxException e) {
                log.error("Invalid repository : " + repository, e);
                downloadService(index + 1, identifier, path, repositories, clients, handler);
                return;
            }
        } else {
            client = clients.get(index);
        }
        final String uri = repository + path;
        downloadFile(index, identifier, path, repositories, clients, handler, client, uri,
            repositories.getJsonObject(index).getString("credential"));
    }

    private void downloadFile(int index, String identifier, String path, JsonArray repositories, List<HttpClient> clients, Handler<AsyncResult<String>> handler, HttpClient client, String uri, String credential) {
        HttpClientRequest req = client.get(uri, resp -> {
            if (resp.statusCode() == 200) {
                resp.bodyHandler(buffer -> {
                    if (uri.endsWith(MAVEN_METADATA_XML)) {
                        try {
                            final String snapshotUri = getSnapshotPath(buffer.toString(),
                                uri.replaceFirst(MAVEN_METADATA_XML, ""), identifier);
                            downloadFile(index, identifier, path, repositories, clients, handler,
                                client, snapshotUri, credential);
                        } catch (Exception e) {
                            log.error("Error reading snapshot metadata", e);
                            handleAsyncError(e, handler);
                        }
                    } else {
                        final String destFile = servicesPath + File.separator + identifier + "-fat.jar";
                        vertx.fileSystem().writeFile(destFile, buffer, ar -> {
                            if (ar.succeeded()) {
                                DefaultAsyncResult.handleAsyncResult(destFile, handler);
                            } else {
                                DefaultAsyncResult.handleAsyncError(ar.cause(), handler);
                            }
                        });
                    }
                });
            } else {
                downloadService(index + 1, identifier, path, repositories, clients, handler);
            }
        });
        if (credential != null) {
            req.putHeader("Authorization", "Basic " + credential);
        }
        req.end();
    }

    private HttpClient createClient(String repository, List<HttpClient> clients) throws URISyntaxException {
        final URI uri = new URI(repository);
        final boolean ssl = "https".equals(uri.getScheme());
        final HttpClientOptions options = new HttpClientOptions()
            .setDefaultHost(uri.getHost())
            .setDefaultPort((uri.getPort() > 0 ? uri.getPort() : (ssl ? 443 : 80)))
            .setSsl(ssl)
            .setKeepAlive(false);
        if (System.getProperty("httpclient.proxyHost") != null) {
            ProxyOptions proxyOptions = new ProxyOptions()
                .setHost(System.getProperty("httpclient.proxyHost"))
                .setPort(Integer.parseInt(System.getProperty("httpclient.proxyPort")))
                .setUsername(System.getProperty("httpclient.proxyUsername"))
                .setPassword(System.getProperty("httpclient.proxyPassword"));
            options.setProxyOptions(proxyOptions);
        }
        final HttpClient client = vertx.createHttpClient(options);
        clients.add(client);
        return client;
    }

    @Override
    public void close() {
        if (releasesClients != null) {
            for (HttpClient c : releasesClients) {
                c.close();
            }
        }
        if (snapshotsClients != null) {
            for (HttpClient c : snapshotsClients) {
                c.close();
            }
        }
    }

    private String getSnapshotPath(String content, String uri, String identifier)
        throws ParserConfigurationException, IOException, SAXException, XPathExpressionException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document xml = builder.parse(new ByteArrayInputStream(content.getBytes("UTF-8")));
        Element root = xml.getDocumentElement();
        XPathFactory xpf = XPathFactory.newInstance();
        XPath xpath = xpf.newXPath();
        String timestamp = xpath.evaluate("//timestamp", root);
        String buildNumber = xpath.evaluate("//buildNumber", root);
        final String [] id = identifier.split("~");
        return uri + id[1] + "-" + id[2].replaceFirst("-SNAPSHOT", "") + "-" + timestamp + "-" + buildNumber + "-fat.jar" ;
    }

}
