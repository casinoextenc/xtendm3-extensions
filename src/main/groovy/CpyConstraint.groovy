/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT030MI.CpyConstraint
 * Description : Copy records to the EXT030 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix 
 * 20230620     FLEBARS      QUAX01 - evol contrainte 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class CpyConstraint extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String NBNR

  public CpyConstraint(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    Integer currentCompany
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }
    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction query = database.table("EXT030").index("00").selection(
        "EXZCID",
        "EXZCOD",
        "EXSTAT",
        "EXCSCD",
        "EXCUNO",
        "EXZCAP",
        "EXZCAS",
        "EXORCO",
        "EXPOPN",
        "EXHIE0",
        "EXHAZI",
        "EXCSNO",
        "EXZALC",
        "EXCFI4",
        "EXZNAG",
        "EXZALI",
        "EXZORI",
        "EXZOHF",
        "EXZPHY",
        "EXRGDT",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
        )
        .build()

    DBContainer EXT030 = query.getContainer()
    EXT030.set("EXCONO", currentCompany)
    EXT030.set("EXZCID", mi.in.get("ZCID"))
    if(query.read(EXT030)) {
      executeCRS165MIRtvNextNumber("ZA", "A")
      EXT030.set("EXZCID",  NBNR as Integer)
      if (!query.read(EXT030)) {
        EXT030.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT030.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        EXT030.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        EXT030.setInt("EXCHNO", 1)
        EXT030.set("EXCHID", program.getUser())
        query.insert(EXT030)
        String constraintID = EXT030.get("EXZCID")
        mi.outData.put("ZCID", constraintID)
        mi.write()
      } else {
        mi.error("L'enregistrement existe déjà")
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  // Execute CRS165MI.RtvNextNumber
  private executeCRS165MIRtvNextNumber(String NBTY, String NBID){
    def parameters = ["NBTY": NBTY, "NBID": NBID]
    Closure<?> handler = { Map<String, String> response ->
      NBNR = response.NBNR.trim()

      if (response.error != null) {
        return mi.error("Failed CRS165MI.RtvNextNumber: "+ response.errorMessage)
      }
    }
    miCaller.call("CRS165MI", "RtvNextNumber", parameters, handler)
  }
}
