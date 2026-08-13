elc-composer-udp-env-dev/
├── dags/
│   └── templates/
│       └── dag_factory.py        # dag factory will be deployed using separate CI/CD pipeline from dedicated repo
│   └── manifests/
│       ├── mtm_manifest.yaml
│       ├── gcc_manifest.yaml
│       └── xyz_manifest.yaml
├── data/
│   ├── etp/
│   │   ├── dbt/                  # This contains all the etp related dbt core project files - mtm, etc
│   │   │   ├── dbt_project.yaml
│   │   │   ├── profiles.yaml
│   │   │   ├── models/
│   │   │   ├── macros/
│   │   │   └── tests/
│   │   └── configs/
│   │       ├── mtm__marketshare__configs.yaml
│   │       └── <domain/wave>__<job>__configs.yaml
│   ├── dig/
│   │   ├── dbt/
│   │   └── configs/
│   └── vch/
│       ├── dbt/
│       └── configs
├── logs/
└── plugins/