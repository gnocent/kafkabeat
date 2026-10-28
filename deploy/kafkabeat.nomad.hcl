# kafkabeat on HashiCorp Nomad: one job per Kafka cluster, Docker driver.
#
# The whole configuration file (credentials included) is a Nomad Variable at
# nomad/jobs/<job name>, readable by this job's tasks without an extra ACL
# policy; set state.path: /alloc/data/state in it. For several clusters, copy
# this file and change the job name (kafkabeat-prod-eu, kafkabeat-prod-us...).
#
#   nomad var put nomad/jobs/kafkabeat config=@kafkabeat.yml
#   nomad job run -var image=registry.example.com/kafkabeat:v0.1.0 kafkabeat.nomad.hcl
#
# See INSTALL.md, section "HashiCorp Nomad".

variable "image" {
  type        = string
  description = "kafkabeat image in a registry the Nomad clients can pull from."
}

variable "datacenters" {
  type    = list(string)
  default = ["*"]
}

job "kafkabeat" {
  datacenters = var.datacenters
  type        = "service"

  group "kafkabeat" {
    # One agent per cluster: two would write the same series.
    count = 1

    # State (end-offset history) kept across restarts on the same node, and
    # copied when the allocation moves, when possible. Losing it only means
    # open lag bounds until the history fills again.
    ephemeral_disk {
      size    = 300
      sticky  = true
      migrate = true
    }

    restart {
      attempts = 10
      interval = "30m"
      delay    = "15s"
      mode     = "delay"
    }

    task "kafkabeat" {
      driver = "docker"

      config {
        image = var.image
        args  = ["-config", "/secrets/kafkabeat.yml"]
      }

      template {
        data        = <<-EOT
          {{- with nomadVar (printf "nomad/jobs/%s" (env "NOMAD_JOB_ID")) }}{{ .config }}{{ end }}
        EOT
        destination = "secrets/kafkabeat.yml"
        perms       = "0400"
        uid         = 65532
        gid         = 65532
        change_mode = "restart"
      }

      # Stops cleanly (final state snapshot) within the grace period.
      kill_timeout = "60s"

      resources {
        cpu        = 500
        memory     = 512
        memory_max = 2048
      }
    }
  }
}
