module "network" {
  source  = "./modules/network"
  project = var.project
}

module "data" {
  source             = "./modules/data"
  project            = var.project
  private_subnet_ids = module.network.private_subnet_ids
  db_sg_id           = module.network.db_sg_id
}

module "services" {
  source  = "./modules/services"
  project = var.project
  region  = var.region

  vpc_id             = module.network.vpc_id
  public_subnet_ids  = module.network.public_subnet_ids
  private_subnet_ids = module.network.private_subnet_ids
  nlb_sg_id          = module.network.nlb_sg_id
  yarp_sg_id         = module.network.yarp_sg_id
  app_sg_id          = module.network.app_sg_id
  data_sg_id         = module.network.data_sg_id
  efs_sg_id          = module.network.efs_sg_id
  vpclink_sg_id      = module.network.vpclink_sg_id

  db_conn_secret_arn     = module.data.conn_string_secret_arn
  pg_password_secret_arn = module.data.pg_password_secret_arn
  pg_conn_secret_arn     = module.data.pg_conn_string_secret_arn

  image_tag            = var.image_tag
  yarp_desired_count   = var.yarp_desired_count
  ingest_desired_count = var.ingest_desired_count
  sub_desired_count    = var.sub_desired_count
}
