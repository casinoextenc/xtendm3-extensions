
/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.AddJokerOrder
 * Description : Adds new Joker order.
 * Date         Changed By   Description
 * 20230526     SEAR         LOG28 - Creation of files and containers
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class AddJokerOrder extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm
  private int currentCompany
  private String orno_input
  private String whlo_input
  private int OOHEAD_ordt
  private int OOHEAD_cudt
  private String OOHEAD_ortp
  private String new_orno
  private double MITAUN_cofa
  private int dmcf
  private String MITMAS_puun

  private String jobNumber

  public AddJokerOrder(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    LocalDateTime timeOfCreation = LocalDateTime.now()

    currentCompany = (Integer)program.getLDAZD().CONO


    //Get mi inputs
    jobNumber = (mi.in.get("BJNO") != null ? (String)mi.in.get("BJNO") : program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss")))
    orno_input = (mi.in.get("ORNO") != null ? (String)mi.in.get("ORNO") : "")
    whlo_input = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")

    //Check if record exists in Table (EXT055)
    DBAction GetqueryEXT055 = database.table("EXT055").index("00").selection("EXBJNO").build()
    DBContainer GetContainerEXT055 = GetqueryEXT055.getContainer()
    GetContainerEXT055.set("EXBJNO", jobNumber)
    //Record exists
    if (!GetqueryEXT055.readAll(GetContainerEXT055, 1, GetEXT055)){
      mi.error("Numéro de job " + jobNumber + " n'existe pas dans la table EXT055")
    }

    DBAction OOHEAD_query = database.table("OOHEAD").index("00").selection("OAORNO","OAORDT","OACUDT", "OAORTP").build()
    DBContainer OOHEAD = OOHEAD_query.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OAORNO", orno_input)
    if(!OOHEAD_query.read(OOHEAD)) {
      mi.error("Le numéro de commande " + orno_input + " n'existe pas")
      return
    } else {
      OOHEAD_ordt = OOHEAD.getInt("OAORDT")
      OOHEAD_cudt = OOHEAD.getInt("OACUDT")
      OOHEAD_ortp = OOHEAD.get("OAORTP")
    }

    executeOIS100MICpyOrder(currentCompany.toString(), orno_input, OOHEAD_ortp, "1", "1", "1")

    DBAction OOHEADUpdate_query = database.table("OOHEAD").index("00").selection("OAORNO").build()
    DBContainer OOHEADUpdate = OOHEADUpdate_query.getContainer()
    OOHEAD.set("OACONO", currentCompany)
    OOHEAD.set("OAORNO", new_orno)

    // Update OOHEAD
    Closure<?> updateCallBack = { LockedResult lockedResult ->
      int changeNumber = lockedResult.get("OACHNO")
      lockedResult.set("OACUDT", OOHEAD_cudt)
      lockedResult.set("OAORDT", OOHEAD_ordt)
      lockedResult.setInt("OALMDT", timeOfCreation.format(DateTimeFormatter.ofPattern("yyyyMMdd")) as Integer)
      lockedResult.setInt("OACHNO", changeNumber + 1)
      lockedResult.set("OACHID", program.getUser())
      lockedResult.update()
    }

    if(!OOHEAD_query.readLock(OOHEAD, updateCallBack)) {
    }

    // list out data
    DBAction ListqueryEXT055 = database.table("EXT055").index("00").selection("EXBJNO","EXITNO","EXZQUV","EXZPQA","EXCOFA").build()
    DBContainer ListContainerEXT055 = ListqueryEXT055.getContainer()
    ListContainerEXT055.set("EXBJNO", jobNumber)
    //Record exists
    if (!ListqueryEXT055.readAll(ListContainerEXT055, 1, ListEXT055)){
    }
    
    // delete workfile
    DBAction DelQuery = database.table("EXT055").index("00").build()
    DBContainer DelcontainerEXT055 = DelQuery.getContainer()
    DelcontainerEXT055.set("EXBJNO", jobNumber)
    if(!DelQuery.readAllLock(DelcontainerEXT055, 1, deleteCallBack)){
    }
    
  }

  // Execute CRS980MI.CpyOrder
  private executeOIS100MICpyOrder(String CONO, String ORNR, String ORTP, String CORH, String RLDT, String CODT) {
    def parameters = ["CONO": CONO, "ORNR": ORNR,  "ORTP": "R01", "CORH": CORH, "RLDT": RLDT, "CODT": CODT]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed OIS100MI.CpyOrder: "+ response.errorMessage)
      }
      new_orno = response.ORNO.trim()
    }
    miCaller.call("OIS100MI", "CpyOrder", parameters, handler)
  }

  Closure<?> ListEXT055 = { DBContainer containerEXT055 ->

    String ItemNumber = containerEXT055.get("EXITNO")
    double quantiteUvcEXT055 = containerEXT055.getDouble("EXZQUV")
    double quantitePalEXT055 = containerEXT055.getDouble("EXZPQA")
	double facteurConversionPalEXT055 = containerEXT055.getDouble("EXCOFA")
    logger.debug("quantiteUvcEXT055 " + quantiteUvcEXT055)
	
	double palQuantity = quantitePalEXT055 * facteurConversionPalEXT055;
	double TotQuantity = 0
	
    /* MITMAS_puun = ""
    DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMPUUN","MMHAZI","MMCFI2","MMSUNO").build()
    DBContainer MITMAS = query_MITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", ItemNumber)
    if(query_MITMAS.read(MITMAS)){
      MITMAS_puun = MITMAS.get("MMPUUN")
    }

    double Quantity = quantitePalEXT055
    double TotQuantity = 0
    DBAction queryMITAUN00 = database.table("MITAUN").index("00").selection(
        "MUCONO",
        "MUITNO",
        "MUAUTP",
        "MUALUN",
        "MUCOFA",
        "MUDMCF"
        ).build()
    DBContainer containerMITAUN = queryMITAUN00.getContainer()
    containerMITAUN.set("MUCONO", currentCompany)
    containerMITAUN.set("MUITNO", ItemNumber)
    containerMITAUN.set("MUAUTP", 1)
    containerMITAUN.set("MUALUN", MITMAS_puun)
    if (queryMITAUN00.read(containerMITAUN)) {
      MITAUN_cofa = containerMITAUN.getDouble("MUCOFA")
      dmcf = containerMITAUN.getInt("MUDMCF")
      if (dmcf == 1) {
        Quantity = quantitePalEXT055 * MITAUN_cofa
      } else {
        Quantity = quantitePalEXT055 / MITAUN_cofa
      }
    }
	TotQuantity = Quantity + quantiteUvcEXT055
	*/
	
	TotQuantity = palQuantity + quantiteUvcEXT055
    
    /* logger.debug("TotQuantity " + TotQuantity.toString() + " and ALUN : " + MITMAS_puun) */
	logger.debug("TotQuantity " + TotQuantity.toString())
    
    // Execute CRS980MI.
    if (TotQuantity > 0) {
      executeOIS100MIAddLineBatchEnt(currentCompany.toString(), new_orno, ItemNumber, TotQuantity.toString(), whlo_input, "UVC")
    }
  }

  private executeOIS100MIAddLineBatchEnt(String CONO, String ORNO, String ITNO, String ORQT, String WHLO, String ALUN){
    def parameters = ["CONO": CONO, "ORNO": ORNO, "ITNO": ITNO, "ORQT": ORQT, "WHLO": WHLO, "ALUN": ALUN]
    Closure<?> handler = { Map<String, String> response ->
      if (response.error != null) {
        return mi.error("Failed OIS100MI.AddLineBatchEnt: "+ response.errorMessage)
      }
    }
    miCaller.call("OIS100MI", "AddLineBatchEnt", parameters, handler)
  }
  
  Closure<?> GetEXT055 = { DBContainer containerEXT055 ->
    return
  }
  
  Closure<?> deleteCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
}


