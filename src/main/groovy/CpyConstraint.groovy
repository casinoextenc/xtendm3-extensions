/**
 * README
 * This extension is used by Mashup
 * QUAX01 Gestion du référentiel qualité
 * Name : EXT030MI.CpyConstraint
 * Description : Copy records to the EXT030 table.
 * Date         Changed By   Description
 * 20230125     SEAR         QUAX01 - Constraints matrix
 * 20230620     FLEBARS      QUAX01 - evol contrainte
 * 20240605     FLEBARS      QUAX01 - Controle code pour validation Infor
 * 20240716     FLEBARS      QUAX01 - Controle code pour validation Infor Retours
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

  private int currentCompany
  private String nbnr

  public CpyConstraint(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer) program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO") as int
      String currentUser = program.getUser()
      if (!checkCompany(currentCompany, currentUser)) {
        mi.error("Company ${currentCompany} does not exist for the user ${currentUser}")
        return
      }
    }

    LocalDateTime timeOfCreation = LocalDateTime.now()
    DBAction ext030Query = database.table("EXT030").index("00").selection(
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

    DBContainer ext030Request = ext030Query.getContainer()
    ext030Request.set("EXCONO", currentCompany)
    ext030Request.set("EXZCID", mi.in.get("ZCID"))
    if (ext030Query.read(ext030Request)) {
      executeCRS165MIRtvNextNumber("ZA", "A")
      ext030Request.set("EXZCID", nbnr as Integer)
      if (!ext030Query.read(ext030Request)) {
        ext030Request.setInt("EXRGDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext030Request.setInt("EXRGTM", timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")) as Integer)
        ext030Request.setInt("EXLMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
        ext030Request.setInt("EXCHNO", 1)
        ext030Request.set("EXCHID", program.getUser())
        ext030Query.insert(ext030Request)
        String constraintID = ext030Request.get("EXZCID")
        mi.outData.put("ZCID", constraintID)
        mi.write()
      } else {
        mi.error("L'enregistrement existe déjà")
        return
      }
    } else {
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  // Execute CRS165MI.RtvNextNumber
  private executeCRS165MIRtvNextNumber(String nbty, String nbid) {
    Map<String, String> parameters = ["NBTY": nbty, "NBID": nbid]
    Closure<?> handler = { Map<String, String> response ->
      nbnr = response.NBNR.trim()

      if (response.error != null) {
        mi.error("Failed CRS165MI.RtvNextNumber: " + response.errorMessage)
        return
      }
    }
    miCaller.call("CRS165MI", "RtvNextNumber", parameters, handler)
  }

  /**
   *  Check if CONO is alowed for user
   * @param cono
   * @param user
   * @return true if alowed false otherwise
   */
  private boolean checkCompany(int cono, String user) {
    DBAction csyusrQuery = database.table("CSYUSR").index("00").build()
    DBContainer csyusrRequest = csyusrQuery.getContainer()
    csyusrRequest.set("CRCONO", cono)
    csyusrRequest.set("CRDIVI", '')
    csyusrRequest.set("CRRESP", user)
    if (!csyusrQuery.read(csyusrRequest)) {
      return false
    }
    return true
  }
}
