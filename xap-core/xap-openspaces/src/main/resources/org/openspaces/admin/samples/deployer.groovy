import org.openspaces.admin.AdminFactory
import org.openspaces.admin.pu.ProcessingUnit
import org.openspaces.admin.pu.ProcessingUnitDeployment
import org.openspaces.admin.space.SpaceDeployment

admin = new AdminFactory().addGroup("kimchy").createAdmin();
admin.gridServiceManagers.waitFor 1
admin.gridServiceContainers.waitFor 2

println "Deploying a space 1"
ProcessingUnit processingUnit = admin.gridServiceManagers.deploy(new SpaceDeployment("test").numberOfInstances(1))
processingUnit.waitFor 1
println "Instance $processingUnit.numberOfInstances, Backups $processingUnit.numberOfBackups"
processingUnit.undeploy()

println "Deploying a space 1,1"
processingUnit = admin.gridServiceManagers.deploy(new SpaceDeployment("test2").numberOfInstances(1).numberOfBackups(1))
processingUnit.waitFor 2
println "Instance $processingUnit.numberOfInstances, Backups $processingUnit.numberOfBackups"
processingUnit.undeploy()

println "Deploying processing unit"
processingUnit = admin.gridServiceManagers.deploy(new ProcessingUnitDeployment("test").name("test3").numberOfInstances(1))
println "name $processingUnit.name"
processingUnit.waitFor 1
println "Instance $processingUnit.numberOfInstances, Backups $processingUnit.numberOfBackups"
processingUnit.incrementInstance()
processingUnit.waitFor 2
sleep 5000
println "Instance $processingUnit.numberOfInstances, Backups $processingUnit.numberOfBackups"
processingUnit.undeploy()

admin.close()

