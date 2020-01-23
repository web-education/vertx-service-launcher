package com.opendigitaleducation.launcher;

import com.opendigitaleducation.launcher.resolvers.ServiceResolverFactory;
import com.opendigitaleducation.launcher.utils.FileUtils;
import io.vertx.core.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.service.ServiceVerticleFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Scanner;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class FolderServiceFactory extends ServiceVerticleFactory {

    protected static final String SERVICES_PATH = "vertx.services.path";
    protected static final String FACTORY_PREFIX = "folderService";
    private static final Logger log = LoggerFactory.getLogger(FolderServiceFactory.class);

    private Vertx vertx;
    private String servicesPath;
    private ServiceResolverFactory serviceResolver;

    @Override
    public void init(Vertx vertx) {
        this.vertx = vertx;
        this.servicesPath = FileUtils.absolutePath(System.getProperty(SERVICES_PATH));
        this.serviceResolver = new ServiceResolverFactory();
        this.serviceResolver.init(vertx, servicesPath);
    }

    @Override
    public void resolve(String id, DeploymentOptions deploymentOptions, ClassLoader classLoader, Future<String> resolution) {
        if (id == null || !id.startsWith(prefix())) {
            resolution.fail("Invalid identifier : " + id);
            return;
        }
        final String identifier = id.substring(prefix().length() + 1);
        String[] artifact = identifier.split("~");
        if (artifact.length != 3) {
           resolution.fail("Invalid artifact : " + identifier);
           return;
        }

        final String servicePath = servicesPath + File.separator +
            identifier + File.separator;
        vertx.fileSystem().exists(servicePath, ar -> {
            if (ar.succeeded() && ar.result()) {
                deploy(identifier, deploymentOptions, classLoader, resolution, artifact, servicePath);
            } else {
                serviceResolver.resolve(identifier, jar -> {
                    if (jar.succeeded()) {
                        unzipJar(vertx, jar.result(), servicePath, res -> {
                            if (res.succeeded()) {
                                deploy(identifier, deploymentOptions, classLoader, resolution, artifact, servicePath);
                            } else {
                                resolution.fail(res.cause());
                            }
                        });
                    } else {
                        resolution.fail("Service not found : " + identifier);
                    }
                });
            }
		});
    }

    private void deploy(String identifier, DeploymentOptions deploymentOptions, ClassLoader classLoader, Future<String> resolution, String[] artifact, String servicePath) {
        vertx.fileSystem().readFile(servicePath + "META-INF" + File.separator + "MANIFEST.MF", ar -> {
			if (ar.succeeded()) {
                Scanner s = new Scanner(ar.result().toString());
                String id = null;
                while (s.hasNextLine()) {
                    final String line = s.nextLine();
                    if (line.contains("Main-Verticle:")) {
                        String [] item = line.split(":");
                        if (item.length == 3) {
                            id = item[2];
                            deploymentOptions.setExtraClasspath(Collections.singletonList(servicePath));
                            deploymentOptions.setIsolationGroup("__vertx_folder_" + artifact[1]);
                            try {
                                URLClassLoader urlClassLoader = new URLClassLoader(
                                    new URL[]{new URL("file://" + servicePath )}, classLoader);
                                FolderServiceFactory.super.resolve(id, deploymentOptions, urlClassLoader, resolution);
                            } catch (MalformedURLException e) {
                                resolution.fail(e);
                            }
                        } else {
                            resolution.fail("Invalid service identifier : " + line);
                        }
                        break;
                    }
                }
                s.close();
                if (id == null && !resolution.isComplete()) {
                    resolution.fail("Service not found : " + identifier);
                }
            } else {
                resolution.fail(ar.cause());
            }
		});
    }

    @Override
    public void close() {
        if (serviceResolver != null) {
            serviceResolver.close();
        }
    }
    //TODO move to utils
    public static void unzipJar(Vertx vertx, String jarFile, String destDir, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            long start = System.currentTimeMillis();
            JarFile jar = null;
            try {
                jar = new JarFile(jarFile);
                Enumeration enumEntries = jar.entries();
                while (enumEntries.hasMoreElements()) {
                    JarEntry file = (JarEntry) enumEntries.nextElement();
                    File f = new File(destDir + File.separator + file.getName());
                    if (file.isDirectory()) {
                        f.mkdirs();
                        continue;
                    }
                    if (!f.getParentFile().exists()) {
                        f.getParentFile().mkdirs();
                    }
                    InputStream is = null;
                    FileOutputStream fos = null;
                    try {
                        is = jar.getInputStream(file);
                        fos = new FileOutputStream(f);
                        byte[] buffer = new byte[8192];
                        int bytesRead;
                        while ((bytesRead = is.read(buffer)) != -1) {
                            fos.write(buffer, 0, bytesRead);
                        }
                    } catch (IOException e) {
                        if (!future.isComplete()) {
                            future.fail(e);
                        }
                    } finally {
                        if (fos != null) {
                            fos.close();
                        }
                        if (is != null) {
                            is.close();
                        }
                    }
                }
            } catch (IOException e) {
                if (!future.isComplete()) {
                    future.fail(e);
                    log.error("Error while unzip jar.", e);
                }
            } finally {
                if (jar != null) {
                    try {
                        jar.close();
                    } catch (IOException e) {
                        log.error("Error closing jar file.", e);
                    }
                }
            }
            log.info(jarFile + " - uncompress duration : " + (System.currentTimeMillis() - start));
            if (!future.isComplete()) {
                future.complete();
            }
        }, handler);
    }

    @Override
    public String prefix() {
        return FACTORY_PREFIX;
    }

}
