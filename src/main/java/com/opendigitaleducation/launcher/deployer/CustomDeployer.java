package com.opendigitaleducation.launcher.deployer;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public interface CustomDeployer {
    boolean canDeploy(JsonObject service);
    void deploy(JsonObject service, Handler<AsyncResult<Void>> result);
}
