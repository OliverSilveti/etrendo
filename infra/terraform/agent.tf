
resource "google_cloud_run_v2_service" "etrendo_agent" {
  name     = "etrendo-agent"
  location = var.region
  project  = local.project_id

  template {
    containers {
      image = "${google_artifact_registry_repository.repo.location}-docker.pkg.dev/${local.project_id}/${google_artifact_registry_repository.repo.repository_id}/etrendo-agent:latest"
      ports {
        container_port = 8080
      }
    }
    service_account = google_service_account.agent_runner.email
  }
}

resource "null_resource" "agent_docker_build_push" {
  triggers = {
    agent_script_hash   = filemd5("../../agent/etrendo-agent/main.py")
    dockerfile_hash     = filemd5("../../agent/etrendo-agent/Dockerfile")
    requirements_hash   = filemd5("../../agent/etrendo-agent/requirements.txt")
  }

  provisioner "local-exec" {
    command = <<-EOT
      gcloud auth configure-docker ${var.region}-docker.pkg.dev
      docker buildx build \
        --platform linux/amd64 \
        -f ../../agent/etrendo-agent/Dockerfile \
        -t ${google_artifact_registry_repository.repo.location}-docker.pkg.dev/${local.project_id}/${google_artifact_registry_repository.repo.repository_id}/etrendo-agent:latest \
        --push ../../agent/etrendo-agent
    EOT
    working_dir = "${path.module}"
  }

  depends_on = [google_artifact_registry_repository.repo]
}
