[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
![Build & Test](https://github.com/infor-cloud/acme-corp-extensions/workflows/Java%20CI/badge.svg?event=push)

# Lttd XtendM3 Extensions modèle de projet
Dépôt modèle pour initialiser les dépôts XtendM3 clients

## Introduction
Ce dépôt est le dépôt modèle qui permet par duplication d'initialiser les dépôts clients pour les modification XtendM3 Extension 


## Setup
Ce projet utilise Maven, Mockito et Junit 4.


La structure du projet est identique à celle de tout projet Maven avec en plus le repertoire des sources groovy

```
.
├── mvnw
├── mvnw.cmd
├── pom.xml
├── README.md
└── src
    ├── main
    │   ├── groovy
    │   │   ├── xxx.groovy
    │   │   └── yyy.groovy
    │   ├── java
    │   └── resources
    │       └── metadata.yaml
    └── test
        ├── groovy
        │   ├── xxxTest.groovy
        │   └── xxxTest.groovy
        └── java
```

### Project Structure Descriptions  

| File/directory name  | Description                                                                                    |
|:---------------------|:-----------------------------------------------------------------------------------------------|
| `mvnw`               | Script d'éxecution maven pour les environnements *nix (linux, unix, ...)                       |
| `mvnw.cmd`           | Script d'éxecution maven pour les environnements windows                                       |
| `pom.xml`            | Maven project definition file                                                                  |
| `README.md`          | Readme file  documentation projet                                                              |
| `src/main/groovy`    | Groovy Extensions source directory                                                             |
| `xxx.groovy`         | Groovy Extension                                                                               |
| `src/main/java`      | Java source directory, **must  always be empty**                                               |
| `src/main/resources` | Resource directory                                                                             |
| `metadata.yaml`      | Extension metadata file, used for trigger definition, **must always be named `metadata.yaml`** |
| `src/test/groovy`    | Groovy Extensions unit test source directory                                                   |
| `xxxTest.groovy`     | Groovy Extension unit test, **name must always follow format `<extension name>Test.groovy`**   |
| `src/test/java`      | Java unit test source directory, **must always be empty**                                      |

### Prerequisites
- Intellij IDEA
- Git

### Instructions
To set up the project locally, perform the following:
- Clone/Download the latest version of project from repository
- Import Maven project project
	- On Eclipse there's an option of importing Maven projects directly
	- On choose either New Project from existing sources or Import project
