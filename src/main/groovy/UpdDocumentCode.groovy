/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT035MI.UpdDocumentCode
 * Description : Add records to the EXT035 table.
 * Date         Changed By   Description
 * 20230201     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class UpdDocumentCode extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final UtilityAPI utility
  private String ads1 = ""

  public UpdDocumentCode(MIAPI mi, DatabaseAPI database, ProgramAPI program, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.utility=utility
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

    //Check if record exists
    DBAction queryEXT035 = database.table("EXT035")
        .index("00")
        .selection(
        "EXCONO",
        "EXZCOD",
        "EXCSCD",
        "EXCUNO",
        "EXDOID",
        "EXADS1",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
        )
        .build()

    DBContainer containerEXT035 = queryEXT035.getContainer()
    containerEXT035.set("EXCONO", currentCompany)
    containerEXT035.set("EXZCOD", mi.in.get("ZCOD"))
    containerEXT035.set("EXCSCD", mi.in.get("CSCD"))
    containerEXT035.set("EXCUNO", mi.in.get("CUNO"))
    containerEXT035.set("EXDOID", mi.in.get("DOID"))

    //Record exists
    if (!queryEXT035.read(containerEXT035)) {
      mi.error("L'enregistrement n'existe pas")
      return
    }

    Closure<?> updateEXT035 = { LockedResult lockedResultEXT035 ->
      lockedResultEXT035.set("EXCSCD", mi.in.get("CSCD"))
      lockedResultEXT035.set("EXCUNO", mi.in.get("CUNO"))
      lockedResultEXT035.set("EXDOID", mi.in.get("DOID"))
      lockedResultEXT035.set("EXADS1", ads1)
      lockedResultEXT035.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      lockedResultEXT035.set("EXCHNO", ((Integer)lockedResultEXT035.get("EXCHNO") + 1))
      lockedResultEXT035.set("EXCHID", program.getUser())
      lockedResultEXT035.update()
    }

    queryEXT035.readLock(containerEXT035, updateEXT035)
  }
}