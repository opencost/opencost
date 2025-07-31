package main

import (
    "fmt"
    "os"
    "github.com/opencost/opencost/pkg/cloud/azure"
)

func main() {
    fmt.Println("Testing Azure authentication...")
    
    // Set up Workload Identity environment
    os.Setenv("AZURE_FEDERATED_TOKEN_FILE", "/tmp/test-token")
    os.Setenv("AZURE_CLIENT_ID", "test-client-id")
    os.Setenv("AZURE_TENANT_ID", "test-tenant-id")
    
    // Try to create credentials (this should trigger our logging)
    holder := &azure.DefaultAzureCredentialHolder{}
    _, err := holder.GetCredential()
    if err != nil {
        fmt.Printf("Error: %v\n", err)
    } else {
        fmt.Println("Credential creation successful")
    }
}
