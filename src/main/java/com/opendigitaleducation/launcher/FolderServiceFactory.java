package com.opendigitaleducation.launcher;

import com.opendigitaleducation.launcher.resolvers.ServiceResolverFactory;
import com.opendigitaleducation.launcher.utils.FileUtils;
import com.opendigitaleducation.launcher.utils.ZipUtils;

import io.vertx.core.*;
import io.vertx.service.ServiceVerticleFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Scanner;

public class FolderServiceFactory extends ServiceVerticleFactory {

    protected static final String SERVICES_PATH = "vertx.services.path";
    public static final String FACTORY_PREFIX = "folderService";

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
    public void resolve(String id, DeploymentOptions deploymentOptions, ClassLoader classLoader, Promise<String> resolution) {
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
                        ZipUtils.unzipJar(vertx, jar.result(), servicePath, res -> {
                            if (res.succeeded()) {
                                deploy(identifier, deploymentOptions, classLoader, resolution, artifact, servicePath);
                            } else {
                                resolution.fail(res.cause());
                            }
                        });
                    } else {
                        resolution.fail("Service not found (JAR): " + identifier);
                    }
                });
            }
		});
    }

    private void deploy(String identifier, DeploymentOptions deploymentOptions, ClassLoader classLoader, Promise<String> resolution, String[] artifact, String servicePath) {
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
                if (id == null && !resolution.future().isComplete()) {
                    resolution.fail("Service not found (MANIFEST): " + identifier);
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

    @Override
    public String prefix() {
        return FACTORY_PREFIX;
    }

}
