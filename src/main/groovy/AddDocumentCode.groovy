/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT035MI.AddDocumentCode
 * Description : Add records to the EXT035 table.
 * Date         Changed By   Description
 * 20230201     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private String ads1 = ""

  public AddDocumentCode(MIAPI mi, DatabaseAPI database, ProgramAPI program, LoggerAPI logger) {
    this.mi = mi
    this.database = database
    this.program = program
    this.logger = logger
  }

  public void main() {
    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Check if record exists in Constraint Code Table (EXT034)
    if (mi.in.get("ZCOD") != null) {
      DBAction queryEXT034 = database.table("EXT034").index("00").build()
      DBContainer EXT034 = queryEXT034.getContainer()
      EXT034.set("EXCONO", currentCompany)
      EXT034.set("EXZCOD", mi.in.get("ZCOD"))
      if (!queryEXT034.read(EXT034)) {
        mi.error("Code contrainte " + mi.in.get("ZCOD") + " n'existe pas")
        return
      }
    }

    //Check if record exists in country Code Table (CSYTAB)
    if (mi.in.get("CSCD") != null) {
      DBAction queryCSYTAB = database.table("CSYTAB").index("00").build()
      DBContainer ContainerCSYTAB = queryCSYTAB.getContainer()
      ContainerCSYTAB.set("CTCONO", currentCompany)
      ContainerCSYTAB.set("CTSTCO", "CSCD")
      ContainerCSYTAB.set("CTSTKY", mi.in.get("CSCD"))
      if (!queryCSYTAB.read(ContainerCSYTAB)) {
        mi.error("Code pays " + mi.in.get("CSCD") + " n'existe pas")
        return
      }
    }

    //Check if record Cutomer in Customer Table (OCUSMA)
    if (mi.in.get("CUNO") != null) {
      DBAction queryOCUSMA = database.table("OCUSMA").index("00").build()
      DBContainer ContainerOCUSMA = queryOCUSMA.getContainer()
      ContainerOCUSMA.set("OKCONO", currentCompany)
      ContainerOCUSMA.set("OKCUNO", mi.in.get("CUNO"))
      if (!queryOCUSMA.read(ContainerOCUSMA)) {
        mi.error("Code client " + mi.in.get("CUNO") + " n'existe pas")
        return
      }
    }

    //Check if record exists in Document Code Table (MPDDOC)
    if (mi.in.get("DOID") != null) {
      DBAction queryMPDDOC = database.table("MPDDOC").index("00").selection("DOADS1").build()
      DBContainer ContainerMPDDOC = queryMPDDOC.getContainer()
      ContainerMPDDOC.set("DOCONO", currentCompany)
      ContainerMPDDOC.set("DODOID", mi.in.get("DOID"))
      if (!queryMPDDOC.read(ContainerMPDDOC)) {
        mi.error("Code Document " + mi.in.get("DOID") + " n'existe pas")
        return
      }
      ads1 = (String)ContainerMPDDOC.get("DOADS1")
    }
    //document type from input
    if (mi.in.get("ADS1") != null) {
      ads1 = mi.in.get("ADS1")
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT035").index("00").build()
    DBContainer EXT035 = query.getContainer()
    EXT035.set("EXCONO", currentCompany)
    EXT035.set("EXZCOD",  mi.in.get("ZCOD"))
    EXT035.set("EXCUNO", mi.in.get("CUNO"))
    EXT035.set("EXCSCD", mi.in.get("CSCD"))
    EXT035.set("EXDOID", mi.in.get("DOID"))
    logger.debug("Avant insert 1")
    if (!query.read(EXT035)) {
      logger.debug("Avant insert 2")
      EXT035.set("EXADS1", ads1)
      EXT035.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT035.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
      EXT035.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      EXT035.setInt("EXCHNO", 1)
      EXT035.set("EXCHID", program.getUser())
      query.insert(EXT035)
      logger.debug("Après insert")
    } else {
      mi.error("L'enregistrement existe déjà")
      return
    }
  }
}