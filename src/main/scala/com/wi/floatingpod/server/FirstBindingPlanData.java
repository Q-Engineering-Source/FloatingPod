package com.wi.floatingpod.server;

import java.util.UUID;

/** schema5/schema6 DATA kernel 共用的只读 recipe 字段。 */
public interface FirstBindingPlanData {
    UUID creationOperationUuid();
    UUID bootstrapOperationUuid();
    UUID prepareWriteUuid();
    UUID certifyWriteUuid();
    UUID bindingUuid();
    int physicalFormat();
    UUID mysqlServerUuid();
    String schema();
    int schemaFormat();
    String ddlSha256();
    UUID provisioningUuid();
}
