import org.openspaces.admin.AdminFactory
import org.openspaces.admin.gsc.GridServiceContainer
import org.openspaces.admin.gsm.GridServiceManager
import org.openspaces.admin.lus.LookupService
import org.openspaces.admin.machine.Machine
import org.openspaces.admin.pu.ProcessingUnit
import org.openspaces.admin.pu.ProcessingUnitInstance
import org.openspaces.admin.pu.events.ProcessingUnitStatusChangedEvent
import org.openspaces.admin.vm.VirtualMachine

admin = new AdminFactory().addGroup("kimchy").createAdmin();
while (true) {
  admin.machines.machineAdded << {Machine machine -> println "Machine [$machine.uid] Added"; }
  admin.machines.machineRemoved << {Machine machine -> println "Machine [$machine.uid] Removed" }
  admin.lookupServices.lookupServiceAdded << {LookupService lookupService -> println "LUS [$lookupService.uid] Added" }
  admin.lookupServices.lookupServiceRemoved << {LookupService lookupService -> println "LUS [$lookupService.uid] Removed" }
  admin.gridServiceContainers.gridServiceContainerAdded << {GridServiceContainer gridServiceContainer -> println "GSC [$gridServiceContainer.uid] Added"}
  admin.gridServiceContainers.gridServiceContainerRemoved << {GridServiceContainer gridServiceContainer -> println "GSC [$gridServiceContainer.uid] Removed"}
  admin.gridServiceManagers.gridServiceManagerAdded << {GridServiceManager gridServiceManger -> println "GSM [$gridServiceManger.uid] Added"}
  admin.gridServiceManagers.gridServiceManagerRemoved << {GridServiceManager gridServiceManger -> println "GSM [$gridServiceManger.uid] Removed"}
  admin.virtualMachines.virtualMachineAdded << {VirtualMachine virtualMachine -> println "VM [$virtualMachine.uid] Added"}
  admin.virtualMachines.virtualMachineRemoved << {VirtualMachine virtualMachine -> println "VM [$virtualMachine.uid] Removed"}

  admin.processingUnits.processingUnitAdded << {ProcessingUnit processingUnit -> println "PU [$processingUnit.name] Added"}
  admin.processingUnits.processingUnitRemoved << {ProcessingUnit processingUnit -> println "PU [$processingUnit.name] Removed"}
  admin.processingUnits.processingUnitStatusChanged << {ProcessingUnitStatusChangedEvent event -> "PU [$event.processingUnit.name] Status changed from [$event.previousStatus] to [$event.newStatus]"}

  admin.processingUnits.processingUnitInstanceAdded << {ProcessingUnitInstance puInstnace -> "PU Instance [$puInstnace.instanceId/$puInstnace.backupId] Added"}
  admin.processingUnits.processingUnitInstanceRemoved << {ProcessingUnitInstance puInstnace -> "PU Instance [$puInstnace.instanceId/$puInstnace.backupId] Removed"}

  Thread.sleep 2000000
}