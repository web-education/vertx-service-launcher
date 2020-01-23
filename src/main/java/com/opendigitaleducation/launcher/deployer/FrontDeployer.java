package com.opendigitaleducation.launcher.deployer;

import com.opendigitaleducation.launcher.FolderServiceFactory;
import com.opendigitaleducation.launcher.resolvers.ServiceResolverFactory;
import com.opendigitaleducation.launcher.utils.DefaultAsyncResult;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

class FrontDeployer implements CustomDeployer {

    private final Vertx vertx;
    private final String servicesPath;
    private final String assetPath;
    private final ServiceResolverFactory serviceResolver;

    static final Map<String, String> outputForTypes = new HashMap<>();
    {
        outputForTypes.put("theme", "themes");
        outputForTypes.put("themes", "themes");
        outputForTypes.put("js", "js");
    }

    public FrontDeployer(Vertx vertx, String servicesPath, String assetPath, ServiceResolverFactory resolver){
        this.vertx = vertx;
        this.assetPath = assetPath;
        this.servicesPath = servicesPath;
        this.serviceResolver = resolver;

    }

    @Override
    public boolean canDeploy(JsonObject service) {
        final String type = service.getString("type");
        return outputForTypes.containsKey(type);
    }

    protected void doDeploy(JsonObject service, String servicePath, Handler<AsyncResult<Void>> result){
        final String type = service.getString("type");
        final String dist = service.getString("dist-dir", "dist");
        final String outputDir = service.getString("output-dir", outputForTypes.getOrDefault(type,""));
        final String distPath = servicePath+ File.separator+dist;
        final String moduleName = service.getString("name");
        final String[] lNameVersion = moduleName.split("~");
        final String assetDir = service.getString("assets-dir", "assets");
        if(lNameVersion.length < 2){
            result.handle(new DefaultAsyncResult<>(new Exception("[FrontDeployer] Invalid name : " + moduleName)));
            return;
        }
        final String outputPath = assetPath + (assetPath.endsWith(File.separator)?"":File.separator) + assetDir
            + File.separator + outputDir + File.separator + lNameVersion[1];
        //clean and recreate
        vertx.fileSystem().deleteRecursive(outputPath,true , resDelete -> {
            vertx.fileSystem().mkdirs(outputPath, resMkdir -> {
                if (resMkdir.succeeded()) {
                    vertx.fileSystem().copyRecursive(distPath, outputPath, true, resCopy -> {
                        if (resCopy.succeeded()) {
                            result.handle(new DefaultAsyncResult<>(null));
                        } else {
                            result.handle(resCopy);
                        }
                    });
                } else {
                    result.handle(resMkdir);
                }
            });
        });
    }

    @Override
    public void deploy(JsonObject service, Handler<AsyncResult<Void>> result) {
        final String id = service.getString("name");
        if (id == null) {
            result.handle(new DefaultAsyncResult<>(new Exception("[FrontDeployer] Invalid identifier : " + id)));
            return;
        }
        String[] artifact = id.split("~");
        if (artifact.length != 3) {
            result.handle(new DefaultAsyncResult<>(new Exception("[FrontDeployer] Invalid artifact : " + id)));
            return;
        }

        final String servicePath = servicesPath + File.separator + id + File.separator;
        vertx.fileSystem().exists(servicePath, ar -> {
            System.out.println("File exists: "+ ar.result()+":"+id);
            if (ar.succeeded() && ar.result()) {
                doDeploy(service, servicePath, result);
            } else {
                serviceResolver.resolve(id, jar -> {
                    if (jar.succeeded()) {
                        FolderServiceFactory.unzipJar(vertx, jar.result(), servicePath, res -> {
                            if (res.succeeded()) {
                                doDeploy(service, servicePath, result);
                            } else {
                                result.handle(new DefaultAsyncResult<>(res.cause()));
                            }
                        });
                    } else {
                        result.handle(new DefaultAsyncResult<>(new Exception("[FrontDeployer] Service not found : " + id)));
                    }
                });
            }
        });
    }
}
