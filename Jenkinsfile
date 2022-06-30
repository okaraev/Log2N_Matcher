def imageName = 'okaraev/matcher'
def appName = 'app_matcher'
def commit(){
    sh(
        script: 'git rev-parse HEAD',
        returnStdout: true
    ).trim()
}
pipeline{
    agent{
        label 'docker'
    }

    stages{
        stage('Checkout'){
            steps{
                checkout([
                    $class: 'GitSCM',
                    branches: scm.branches,
                    doGenerateSubmoduleConfigurations: scm.doGenerateSubmoduleConfigurations,
                    extensions: scm.extensions + [[$class: 'CloneOption', noTags: false, reference: '', shallow: true]],
                    submoduleCfg: [],
                    userRemoteConfigs: scm.userRemoteConfigs
                ])
            }
        }
        
        stage('Pre-Integration Tests'){
            steps{
                script{                
                    def testImage = docker.build("${imageName}-test:pipeline","-f ${appName}-test.df .")
                    parallel(
                        'Code Quality Test': {
                            sh "docker run --rm ${imageName}-test:pipeline golint"
                        },
                        'Dockerfile test' : {
                            sh "docker run --rm -i hadolint/hadolint:latest < ${appName}-test.df"
                            sh "docker run --rm -i hadolint/hadolint:latest < ${appName}.df"
                        }
                    )
                }
            }
        }

        stage('Unit Tests'){
            steps{
                script{
                    sh "docker run --rm ${imageName}-test:pipeline go test"
                }
            }
        }

        stage('Build'){
            steps{
                script{                    
                    docker.build(imageName,"-f ${appName}.df .")
                }
            }
        }

        stage('Push'){    
            steps{
                script{
                    commit = commit()
                    docker.withRegistry('', 'registry') {
                        docker.image(imageName).push(commit)
                        docker.image(imageName).push("latest-${env.BRANCH_NAME}")
                    }
                }   
            }
        }

        stage('Invoke CD'){
            steps{
                script{
                    if(env.BRANCH_NAME == 'master'){
                        timeout(time: 2, unit: "HOURS") {
                            input message: "Approve Deploy?", ok: "Yes"
                        }
                        build job: "Log2N/CD/${env.BRANCH_NAME}"
                    }
                }
            }
        }
    }
}