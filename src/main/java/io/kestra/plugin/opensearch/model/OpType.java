package io.kestra.plugin.opensearch.model;

public enum OpType {
    INDEX,
    CREATE,
    UPDATE,
    DELETE;

    public org.opensearch.client.opensearch._types.OpType to() {
        return switch (this) {
            case INDEX -> org.opensearch.client.opensearch._types.OpType.Index;
            case CREATE -> org.opensearch.client.opensearch._types.OpType.Create;
            case UPDATE, DELETE -> throw new IllegalArgumentException(
                "opType " + this + " is not supported for a single-document request, only INDEX and CREATE are; use Load or Bulk for UPDATE and DELETE"
            );
        };
    }
}
