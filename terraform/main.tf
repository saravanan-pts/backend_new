terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }
  backend "azurerm" {}
}

# FIXED: Explicitly tell Terraform to use the AKS credentials downloaded by 'az aks get-credentials'
provider "kubernetes" {
  config_path = "~/.kube/config"
}

# --- Variables (No changes needed) ---
variable "container_image" { type = string }
variable "openai_endpoint" { type = string }
variable "openai_key" { 
  type      = string
  sensitive = true 
}
variable "openai_deployment" { type = string }
variable "openai_embedding"  { type = string }
variable "openai_generation" { type = string }
variable "openai_version"    { type = string }
variable "cosmos_endpoint"   { type = string }
variable "cosmos_db"         { type = string }
variable "cosmos_container"  { type = string }
variable "cosmos_key" { 
  type      = string
  sensitive = true 
}

# --- Deployment ---
resource "kubernetes_deployment" "backend" {
  metadata {
    name   = "irmai-kg-backend"
    labels = { app = "irmai-kg-backend" }
  }
  spec {
    replicas = 1
    selector { match_labels = { app = "irmai-kg-backend" } }
    template {
      metadata { labels = { app = "irmai-kg-backend" } }
      spec {
        container {
          name  = "backend"
          image = var.container_image
          port { container_port = 8000 }
          
          env {
            name  = "AZURE_OPENAI_ENDPOINT"
            value = var.openai_endpoint
          }
          env {
            name  = "AZURE_OPENAI_API_KEY"
            value = var.openai_key
          }
          env {
            name  = "AZURE_OPENAI_DEPLOYMENT_NAME"
            value = var.openai_deployment
          }
          env {
            name  = "AZURE_OPENAI_EMBEDDING_MODEL"
            value = var.openai_embedding
          }
          env {
            name  = "AZURE_OPENAI_GENERATION_MODEL"
            value = var.openai_generation
          }
          env {
            name  = "AZURE_OPENAI_API_VERSION"
            value = var.openai_version
          }
          env {
            name  = "COSMOS_GREMLIN_ENDPOINT"
            value = var.cosmos_endpoint
          }
          env {
            name  = "COSMOS_GREMLIN_DATABASE"
            value = var.cosmos_db
          }
          env {
            name  = "COSMOS_GREMLIN_CONTAINER"
            value = var.cosmos_container
          }
          env {
            name  = "COSMOS_GREMLIN_KEY"
            value = var.cosmos_key
          }
        }
      }
    }
  }
}

# --- Service ---
resource "kubernetes_service" "backend" {
  metadata { 
    name = "irmai-kg-backend-svc" 
    # Optional: If you are using Azure, Cloudflare might prefer specific annotations 
    # for the LoadBalancer, but the port mapping below is the primary fix.
  }
  spec {
    selector = { app = "irmai-kg-backend" }
    
    # Standard HTTP Port
    port {
      name        = "http"
      port        = 80
      target_port = 8000
      protocol    = "TCP"
    }

    # FIXED: Added Port 443 mapping for Cloudflare HTTPS (Solves 522 error)
    port {
      name        = "https"
      port        = 443
      target_port = 8000
      protocol    = "TCP"
    }

    type = "LoadBalancer"
  }
}
