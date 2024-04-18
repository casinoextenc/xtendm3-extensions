/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT035MI.CpyDocumentCode
 * Description : Copy records to the EXT035 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction

  public CpyDocumentCode(MIAPI mi, DatabaseAPI database, ProgramAPI program) {
    this.mi = mi
    this.database = database
    this.program = program
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

    //Check if record exists in Constraint Code Table (EXT034)
    if (mi.in.get("CZCO") != null) {
      DBAction queryEXT034 = database.table("EXT034").index("00").build()
      DBContainer EXT034 = queryEXT034.getContainer()
      EXT034.set("EXCONO", currentCompany)
      EXT034.set("EXZCOD", mi.in.get("CZCO"))
      if (!queryEXT034.read(EXT034)) {
        mi.error("Code contrainte (to) " + mi.in.get("CZCO") + " n'existe pas")
        return
      }
    }

    //Check if record exists in country Code Table (CSYTAB)
    if (mi.in.get("ZCSC") != null) {
      DBAction queryCSYTAB = database.table("CSYTAB").index("00").build()
      DBContainer ContainerCSYTAB = queryCSYTAB.getContainer()
      ContainerCSYTAB.set("CTCONO", currentCompany)
      ContainerCSYTAB.set("CTSTCO", "CSCD")
      ContainerCSYTAB.set("CTSTKY", mi.in.get("ZCSC"))
      if (!queryCSYTAB.read(ContainerCSYTAB)) {
        mi.error("Code pays (to) " + mi.in.get("ZCSC") + " n'existe pas")
        return
      }
    }

    //Check if record Cutomer in Customer Table (OCUSMA)
    if (mi.in.get("ZCUN") != null) {
      DBAction queryOCUSMA = database.table("OCUSMA").index("00").build()
      DBContainer ContainerOCUSMA = queryOCUSMA.getContainer()
      ContainerOCUSMA.set("OKCONO", currentCompany)
      ContainerOCUSMA.set("OKCUNO", mi.in.get("ZCUN"))
      if (!queryOCUSMA.read(ContainerOCUSMA)) {
        mi.error("Code client (to) " + mi.in.get("ZCUN") + " n'existe pas")
        return
      }
    }

    //Check if record exists in Document Code Table (MPDDOC)
    if (mi.in.get("ZDOI") != null) {
      DBAction queryMPDDOC = database.table("MPDDOC").index("00").selection("DOADS1").build()
      DBContainer ContainerMPDDOC = queryMPDDOC.getContainer()
      ContainerMPDDOC.set("DOCONO", currentCompany)
      ContainerMPDDOC.set("DODOID", mi.in.get("ZDOI"))
      if (!queryMPDDOC.read(ContainerMPDDOC)) {
        mi.error("Code Document (to) " + mi.in.get("ZDOI") + " n'existe pas")
        return
      }
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT035").index("00").selection("EXCSCD", "EXCUNO", "EXDOID", "EXADS1").build()
    DBContainer EXT035 = query.getContainer()
    EXT035.set("EXCONO", currentCompany)
    EXT035.set("EXZCOD", mi.in.get("ZCOD"))
    EXT035.set("EXCSCD", mi.in.get("CSCD"))
    EXT035.set("EXCUNO", mi.in.get("CUNO"))
    EXT035.set("EXDOID", mi.in.get("DOID"))
    if(query.read(EXT035)){
      EXT035.set("EXZCOD", mi.in.get("CZCO"))
      EXT035.set("EXCSCD", mi.in.get("ZCSC"))
      EXT035.set("EXCUNO", mi.in.get("ZCUN"))
      EXT035.set("EXDOID", mi.in.get("ZDOI"))
      if (!query.read(EXT035)) {
        EXT035.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT035.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT035.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT035.setInt("EXCHNO", 1)
        EXT035.set("EXCHID", program.getUser())
        query.insert(EXT035)
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }
}
