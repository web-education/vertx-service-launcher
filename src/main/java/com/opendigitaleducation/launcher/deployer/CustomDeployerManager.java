package com.opendigitaleducation.launcher.deployer;

import com.opendigitaleducation.launcher.resolvers.ServiceResolverFactory;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

public class CustomDeployerManager implements  CustomDeployer{
    private final Vertx vertx;
    private final String servicesPath;
    private final String assetPath;
    private final ServiceResolverFactory serviceResolver;
    private final List<CustomDeployer> deployers = new ArrayList<>();

    public CustomDeployerManager(Vertx vertx, String servicesPath, String assetPath){
        this.vertx = vertx;
        this.assetPath = assetPath;
        this.servicesPath = servicesPath;
        this.serviceResolver = new ServiceResolverFactory();
        this.serviceResolver.init(vertx, servicesPath);
        deployers.add(new FrontDeployer(vertx, servicesPath, assetPath, serviceResolver));
    }

    public boolean canDeploy(JsonObject service){
        for(CustomDeployer deployer : deployers){
            if(deployer.canDeploy(service)) return true;
        }
        return false;
    }

    public void deploy(JsonObject service, Handler<AsyncResult<Void>> result){
        for(CustomDeployer deployer : deployers){
            if(deployer.canDeploy(service)) {
                deployer.deploy(service, result);
                return;
            }
        }
    }
}
