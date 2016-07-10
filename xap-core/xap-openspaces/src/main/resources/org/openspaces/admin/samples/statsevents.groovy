import org.openspaces.admin.AdminFactory
import org.openspaces.admin.space.events.SpaceInstanceStatisticsChangedEvent
import org.openspaces.admin.transport.events.TransportStatisticsChangedEvent
import org.openspaces.admin.vm.events.VirtualMachineStatisticsChangedEvent

admin = new AdminFactory().addGroup("kimchy").createAdmin();
while (true) {

  admin.startStatisticsMonitor()
  admin.spaces.spaceInstanceStatisticsChanged << {SpaceInstanceStatisticsChangedEvent event -> println "Space Instance [$event.spaceInstance.uid] Stats: write [$event.statistics.writeCount]"}
  admin.virtualMachines.virtualMachineStatisticsChanged << {VirtualMachineStatisticsChangedEvent event -> println "VM [$event.virtualMachine.uid] Heap Used [$event.statistics.memoryHeapUsedInBytes]"}
  admin.transports.transportStatisticsChanged << {TransportStatisticsChangedEvent event -> println "Transport [$event.transport.uid] [$event.statistics.completedTaskCount]"}
  Thread.sleep 2000000
}