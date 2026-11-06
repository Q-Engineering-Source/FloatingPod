package com.wi.floatingpod.server;

import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

/** 严格 schema6 recipe DATA，不携带 native、SQL 或 readiness 权限。 */
public record DbFirstWorldAdoptionPlanData(
        UUID creationOperationUuid,
        UUID bootstrapOperationUuid,
        UUID prepareWriteUuid,
        UUID certifyWriteUuid,
        UUID bindingUuid,
        int physicalFormat,
        UUID mysqlServerUuid,
        String schema,
        int schemaFormat,
        String ddlSha256,
        UUID provisioningUuid) implements FirstBindingPlanData {

    public DbFirstWorldAdoptionPlanData {
        Objects.requireNonNull(creationOperationUuid, "creationOperationUuid");
        Objects.requireNonNull(bootstrapOperationUuid, "bootstrapOperationUuid");
        Objects.requireNonNull(prepareWriteUuid, "prepareWriteUuid");
        Objects.requireNonNull(certifyWriteUuid, "certifyWriteUuid");
        Objects.requireNonNull(bindingUuid, "bindingUuid");
        Objects.requireNonNull(mysqlServerUuid, "mysqlServerUuid");
        Objects.requireNonNull(provisioningUuid, "provisioningUuid");
        if (physicalFormat != 2 || schemaFormat != 6 || bindingUuid.version() != 4) {
            throw new IllegalArgumentException("DB-first recipe requires schema6, physical2 and UUIDv4 binding");
        }
        if (schema == null || !schema.matches("[a-z][a-z0-9_]{0,63}")) {
            throw new IllegalArgumentException("Schema6 recipe requires a canonical lowercase safe schema");
        }
        UUID zero = new UUID(0, 0);
        for (UUID identity : new UUID[] {creationOperationUuid, bootstrapOperationUuid, prepareWriteUuid,
                certifyWriteUuid, bindingUuid, mysqlServerUuid, provisioningUuid}) {
            if (identity.equals(zero)) {
                throw new IllegalArgumentException("Schema6 recipe requires nonzero UUID identities");
            }
        }
        if (prepareWriteUuid.equals(certifyWriteUuid)) {
            throw new IllegalArgumentException("Schema6 prepare and certify write UUIDs must be distinct");
        }
        WindowsWorldProvisioner.hashBytes(ddlSha256);
        ddlSha256 = ddlSha256.toUpperCase(Locale.ROOT);
    }
}
