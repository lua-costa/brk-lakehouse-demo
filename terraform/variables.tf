variable "project_id" {
  type        = string
  description = "ID do Projeto GCP recém-criado"
}

variable "region" {
  type        = string
  description = "Região padrão dos recursos GCP"
  default     = "us-east4"
}

variable "billing_account_id" {
  type        = string
  description = "ID da Conta de Faturamento GCP"
}

variable "environment" {
  type        = string
  description = "Ambiente de implantação"
  default     = "demo"
}
