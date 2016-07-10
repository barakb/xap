import org.openspaces.admin.AdminFactory

admin = new AdminFactory().addGroup("kimchy").createAdmin();
while (true) {
  admin.gridServiceManagers.each { println "GSM: $it.uid" }
  Thread.sleep 1000
}