terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }
  backend "azurerm" {}
}

provider "kubernetes" {
  config_path = "~/.kube/config"
}

variable "container_image" { type = string }
variable "openai_key" { type = string }
variable "cosmos_key" { type = string }

resource "kubernetes_deployment" "backend" {
  metadata {
    name = "irmai-kg-backend"
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
          
          # Environment variables for app/config.py
          env { name = "AZURE_OPENAI_API_KEY", value = var.openai_key }
          env { name = "COSMOS_GREMLIN_KEY", value = var.cosmos_key }
          # Add other variables like COSMOS_GREMLIN_ENDPOINT here
        }
      }
    }
  }
}

resource "kubernetes_service" "backend" {
  metadata { name = "irmai-kg-backend-svc" }
  spec {
    selector = { app = "irmai-kg-backend" }
    port {
      port        = 80
      target_port = 8000
    }
    type = "LoadBalancer"
  }
}