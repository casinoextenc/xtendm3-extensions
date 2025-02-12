/**
 * This extension is used by Mashup
 * Name : EXT023MI.AddAssortItems
 * COMX01 Gestion des assortiments clients
 * Description : Read EXT022 table and call "CRS105MI/AddAssmItem" for each item
 * Date         Changed By   Description
 * 20220112     YBLUTEAU     COMX01- Add assortment
 * 20240620     FLEBARS       COMX01 - Controle code pour validation Infor
 * 20240709     FLEBARS       COMX01 - Controle code pour validation Infor retours
 */
public class AddAssortItems extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final MICallerAPI miCaller
  private final ProgramAPI program
  private final UtilityAPI utility

  private int currentCompany
  private String ascd = ""
  private String cuno = ""
  private String fdat = ""
  private String itno = ""

  private boolean in60 = false
  private String msgd = ""

  public AddAssortItems(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, UtilityAPI utility, MICallerAPI miCaller) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
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
    ascd = mi.in.get("ASCD")
    cuno = mi.in.get("CUNO")

    fdat = ""
    if (mi.in.get("FDAT") == null) {
      mi.error("Date de début est obligatoire")
      return
    } else {
      fdat = mi.in.get("FDAT")
      if (!utility.call("DateUtil", "isDateValid", fdat, "yyyyMMdd")) {
        mi.error("Date de début est invalide")
        return
      }
    }

    // Check selection header
    DBAction ext020Query = database.table("EXT020").index("00").build()
    DBContainer ext020Request = ext020Query.getContainer()
    ext020Request.set("EXCONO", currentCompany)
    ext020Request.set("EXASCD", ascd)
    ext020Request.set("EXCUNO", cuno)
    ext020Request.setInt("EXFDAT", fdat as Integer)
    if (!ext020Query.read(ext020Request)) {
      mi.error("Entête sélection n'existe pas")
      return
    }

    DBAction ext022Query = database.table("EXT022").index("00").selection("EXITNO").build()
    DBContainer ext022Request = ext022Query.getContainer()
    ext022Request.set("EXCONO", currentCompany)
    ext022Request.set("EXASCD", ascd)
    ext022Request.set("EXCUNO", cuno)
    ext022Request.set("EXFDAT", fdat as Integer)
    Closure<?> ext022Reader = { DBContainer ext022Result ->
      itno = ext022Result.get("EXITNO")
      executeCRS105MIAddAssmItem(ascd, itno, fdat)
      if (in60) {
        mi.error(msgd)
        return
      }
    }
    if (!ext022Query.readAll(ext022Request, 4, 1000, ext022Reader)) {
      mi.error("Enregistrement n'existe pas")
      return
    }
  }


  /**
   * Call CRS105MI.AddAssmItem
   * @param ASCD
   * @param ITNO
   * @param FDAT
   */
  private void executeCRS105MIAddAssmItem(String ASCD, String ITNO, String FDAT) {
    Map<String, String> parameters = ["ASCD": ASCD, "ITNO": ITNO, "FDAT": FDAT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        in60 = true
        msgd = response.errorMessage
        return
      }
    }
    miCaller.call("CRS105MI", "AddAssmItem", parameters, handler)
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
