terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }
  # This enables storing the 'state' file in Azure Storage so multiple 
  # pipeline runs can track what has already been deployed.
  backend "azurerm" {}
}

provider "kubernetes" {
  # No config_path needed here. The GitLab pipeline runs 'az aks get-credentials' 
  # right before terraform runs, which sets up the local authentication automatically.
}

# --- Variable Definitions ---
# These are passed from .gitlab-ci.yml using the -var flag

variable "container_image" {
  description = "The full path to the image in ACR, including the tag"
  type        = string
}

variable "openai_endpoint" {
  type = string
}

variable "openai_key" {
  type      = string
  sensitive = true
}

variable "openai_deployment" {
  type = string
}

variable "openai_embedding" {
  type = string
}

variable "openai_generation" {
  type = string
}

variable "openai_version" {
  type = string
}

variable "cosmos_endpoint" {
  type = string
}

variable "cosmos_db" {
  type = string
}

variable "cosmos_container" {
  type = string
}

variable "cosmos_key" {
  type      = string
  sensitive = true
}

# --- Kubernetes Deployment ---
# This describes the "brain" of your backend running in AKS.

resource "kubernetes_deployment" "backend" {
  metadata {
    name = "irmai-kg-backend"
    labels = {
      app = "irmai-kg-backend"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "irmai-kg-backend"
      }
    }

    template {
      metadata {
        labels = {
          app = "irmai-kg-backend"
        }
      }

      spec {
        container {
          name  = "backend"
          image = var.container_image
          
          # Internal port the FastAPI app listens on (defined in Dockerfile)
          port {
            container_port = 8000
          }

          # Environment Variables: These are read by your Python app's 'Settings' class.
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

# --- Kubernetes Service ---
# This creates the "Public Doorway" (LoadBalancer) for your backend.

resource "kubernetes_service" "backend" {
  metadata {
    name = "irmai-kg-backend-svc"
  }

  spec {
    selector = {
      app = "irmai-kg-backend"
    }

    port {
      # The public port Jennings will use for 'api-kg.irmai.io'
      port        = 80
      # The internal port on the container we are mapping to
      target_port = 8000
    }

    type = "LoadBalancer"
  }
}