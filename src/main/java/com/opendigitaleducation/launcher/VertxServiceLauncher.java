package com.opendigitaleducation.launcher;

import com.opendigitaleducation.launcher.deployer.CustomDeployer;
import com.opendigitaleducation.launcher.deployer.CustomDeployerManager;
import com.opendigitaleducation.launcher.utils.FileUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.LocalMap;

import java.io.File;

import static com.opendigitaleducation.launcher.FolderServiceFactory.FACTORY_PREFIX;

public class VertxServiceLauncher extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(VertxServiceLauncher.class);

    private String node;
    private boolean cluster;
    private LocalMap<String, String> versionMap;
    private LocalMap<String, String> deploymentsIdMap;
    private CustomDeployer customDeployer ;

    @Override
    public void start() throws Exception {
        JsonArray services = config().getJsonArray("services");
        if (services == null || services.size() == 0) {
            log.error("Missing services to deploy.");
            return;
        }

        final LocalMap<Object, Object> serverMap = vertx.sharedData().getLocalMap("server");
        cluster = config().getBoolean("cluster", false);
        serverMap.put("cluster", cluster);
        node = config().getString("node", "");
        serverMap.put("node", node);
        deploymentsIdMap = vertx.sharedData().getLocalMap("deploymentsId");
        versionMap = vertx.sharedData().getLocalMap("versions");
        final String servicesPath = FileUtils.absolutePath(System.getProperty("vertx.services.path"));
        customDeployer = new CustomDeployerManager(vertx,servicesPath,config().getString("assets-path"));
        deployServices(services);
    }

    private void deployServices(JsonArray services) {
        deployServices(services, 0);
    }

    private void deployServices(JsonArray services, final int index) {
        if (index >= services.size()) return;
        final JsonObject service = services.getJsonObject(index);
        final String name = service.getString("name");
        if (name == null || name.isEmpty()) {
			deployServices(services, index + 1);
		}
        //custom deployer
        if(customDeployer.canDeploy(service)){
            if (service.getBoolean("waitDeploy", false)){
                customDeployer.deploy(service, res->{
                    if(res.succeeded()){
                        log.info("[CustomDeployer] Deployed succeed :"+name);
                        deployServices(services, index + 1);
                    }else{
                        log.error("[CustomDeployer] Error deploying :"+name, res.cause());
                    }
                });
            }else{
                customDeployer.deploy(service, res->{
                    if(res.succeeded()){
                        log.info("[CustomDeployer] Deployed succeed :"+name);
                    }else{
                        log.error("[CustomDeployer] Error deploying :"+name, res.cause());
                    }
                });
                deployServices(services, index + 1);
            }
            return;
        }
        //
        JsonObject config = service.getJsonObject("config");
        if (config == null) {
            config = new JsonObject();
        }
        config.put("cwd", FileUtils.absolutePath(System.getProperty("vertx.services.path")) +
            File.separator + name);
        if (!config.containsKey("assets-path") && config().getString("assets-path") != null) {
            config.put("assets-path", config().getString("assets-path"));
        }
        final String address = config.getString("address");
        if (cluster && !node.isEmpty() && address != null) {
            config.put("address", node + address);
        }
        final DeploymentOptions deploymentOptions = new DeploymentOptions()
            .setConfig(config)
            .setWorker(service.getBoolean("worker", false))
            .setInstances(config.getInteger("instances", 1))
            .setMultiThreaded(service.getBoolean("multi-threaded", false));
        if (service.getBoolean("waitDeploy", false)) {
            vertx.deployVerticle(FACTORY_PREFIX + ":" + name, deploymentOptions, ar -> {
                if (ar.succeeded()) {
                    deployServices(services, index + 1);
                    addAppVersion(name, ar.result());
                } else {
                    log.error("Error deploying required service  : " + name, ar.cause());
                }
            });
        } else {
            vertx.deployVerticle(FACTORY_PREFIX + ":" + name, deploymentOptions, ar -> {
                if (ar.failed()) {
                    log.error("Error deploying required service  : " + name, ar.cause());
                } else {
                    addAppVersion(name, ar.result());
                }
            });
            deployServices(services, index + 1);
        }
    }

    private void addAppVersion(final String moduleName, final String deploymentId) {
        final String[] lNameVersion = moduleName.split("~");
        if (lNameVersion.length == 3) {
            versionMap.put(lNameVersion[0] + "." + lNameVersion[1], lNameVersion[2]);
            deploymentsIdMap.put(lNameVersion[0] + "." + lNameVersion[1], deploymentId);
        }
    }

}
