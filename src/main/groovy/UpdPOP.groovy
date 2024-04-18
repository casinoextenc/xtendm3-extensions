/**
 * Name : EXT850MI.UpdPOP
 *
 * Description :
 * This API method to Update MPOPLP.POPPSQ
 * There are no standard transactions that allow us to update this field. We have created a case 17675366
 *
 * Date         Changed By    Description
 * 20230804     FLEBARS       Creation
 */
class UpdPOP extends ExtendM3Transaction {
  private final MIAPI mi
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final ProgramAPI program
  private final UtilityAPI utility
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private int currentCompany
  private String errorMessage
  private String itds
  private String NBNR
  private Integer line

  public UpdPOP(MIAPI mi, DatabaseAPI database, LoggerAPI logger, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.mi = mi
    this.database = database
    this.logger = logger
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    currentCompany = (Integer)program.getLDAZD().CONO
    boolean hasStdError = false

    //Get mi inputs
    String plpn = (mi.in.get("PLPN") != null ? (String)mi.in.get("PLPN") : 0)
    String plps = (mi.in.get("PLPS") != null ? (String)mi.in.get("PLPS") : 0)
    String plp2 = (mi.in.get("PLP2") != null ? (String)mi.in.get("PLP2") : 0)
    String ppsq = (String)(mi.in.get("PPSQ") != null ? mi.in.get("PPSQ") : "")
    String ptxt = ""

    //read mpoplp to read note
    DBAction queryMPOPLP00 = database.table("MPOPLP").index("00").selection(
        "POCONO"
        ,"POPLPN"
        ,"POPLPS"
        ,"POPLP2"
        ,"POPTXT"
        ).build()

    DBContainer containerMPOPLP = queryMPOPLP00.getContainer()
    containerMPOPLP.set("POCONO",currentCompany)
    containerMPOPLP.set("POPLPN",plpn as int)
    containerMPOPLP.set("POPLPS",plps as int)
    containerMPOPLP.set("POPLP2",plp2 as int)
    if (queryMPOPLP00.read(containerMPOPLP)) {
      ptxt = containerMPOPLP.getString("POPTXT").trim()
    }

    def params = [
      "PLPN" : plpn + ""
      ,"PLPS" : plps + ""
      ,"PLP2" : plp2 + ""
      ,"NOTE" : ptxt + "."
    ]
    def callback = { Map<String, String> response ->
      logger.debug("Call PPS170MI/UpdPOP " + response)
      if (response.error != null) {
        hasStdError = true
        return mi.error(response.errorMessage)
      }
    }
    miCaller.call("PPS170MI","UpdPOP", params, callback)
    Closure<?> updaterMPOPLP00 = { LockedResult lockedResultMPOPLP00 ->
      lockedResultMPOPLP00.set("POPPSQ", ppsq)
      lockedResultMPOPLP00.set("POLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
      lockedResultMPOPLP00.set("POCHNO", ((Integer)lockedResultMPOPLP00.get("POCHNO") + 1))
      lockedResultMPOPLP00.set("POCHID", program.getUser())
      lockedResultMPOPLP00.update()
    }
    if (hasStdError)
      return
    if (queryMPOPLP00.readLock(containerMPOPLP, updaterMPOPLP00)) {
    }
  }
}