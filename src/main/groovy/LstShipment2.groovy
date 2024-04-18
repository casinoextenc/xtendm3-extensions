/**
 * README
 * This extension is used by Mashup
 *
 * Name : EXT050MI.LstShipment2
 * Description : List shipment
 * Date         Changed By   Description
 * 20230601     SEAR         LOG28 - Creation of files and containers
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

public class LstShipment2 extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility
  private String parm
  private String whlo_OOLINE
  private String whlo_input
  private long dlix_input
  private long dlix_MHDISH
  private int conn_MHDISH
  private String ude1_input
  private int ccud_input
  private int currentCompany
  private boolean sameUDE1
  private boolean sameCCUD
  private Boolean found_DRADTR
  private long index = 0
  private int shipment = 0
  private String dossier = ""
  private String semaine = ""
  private String annee = ""
  private int arrivalDate = 0
  private int departureDate = 0
  private int packagindDate = 0
  
  private long ship = 0

  private String jobNumber

  public LstShipment2(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {
    LocalDateTime timeOfCreation = LocalDateTime.now()
    jobNumber = program.getJobNumber() + timeOfCreation.format(DateTimeFormatter.ofPattern("yyMMdd")) + timeOfCreation.format(DateTimeFormatter.ofPattern("HHmmss"))


    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    //Get mi inputs
    whlo_input = (mi.in.get("WHLO") != null ? (String)mi.in.get("WHLO") : "")
    ude1_input = (mi.in.get("UDE1") != null ? (String)mi.in.get("UDE1") : "")
    dlix_input = (Long)(mi.in.get("DLIX") != null ? mi.in.get("DLIX") : 0)
    ccud_input = (int)(mi.in.get("CCUD") != null ? mi.in.get("CCUD") : 0)

    // check warehouse
    DBAction query_MITWHL = database.table("MITWHL").index("00").selection("MWWHLO").build()
    DBContainer MITWHL = query_MITWHL.getContainer()
    MITWHL.set("MWCONO", currentCompany)
    MITWHL.set("MWWHLO", whlo_input)
    if(!query_MITWHL.read(MITWHL)){
      mi.error("Le dépôt " + whlo_input + " n'existe pas")
      return
    }

    ExpressionFactory expression_MHDISH = database.getExpressionFactory("MHDISH")
    expression_MHDISH = (expression_MHDISH.ne("OQCONN", "0"))
    if(mi.in.get("DLIX") != null){
      expression_MHDISH = expression_MHDISH.and(expression_MHDISH.eq("OQDLIX", dlix_input.toString().trim()))
    }
    expression_MHDISH = expression_MHDISH.and(expression_MHDISH.eq("OQRLDT", "0"))
	  expression_MHDISH = expression_MHDISH.and(expression_MHDISH.lt("OQPGRS", "50"))

    DBAction query_MHDISH = database.table("MHDISH").index("94").matching(expression_MHDISH).selection("OQDLIX","OQCONN").build()
    DBContainer containerMHDISH = query_MHDISH.getContainer()
    containerMHDISH.set("OQCONO", currentCompany)
    containerMHDISH.set("OQINOU", 1)
    containerMHDISH.set("OQWHLO", whlo_input)
	

    if (query_MHDISH.readAll(containerMHDISH, 3, MHDISHData)){
    }
    
    // list out data
    DBAction ListqueryEXT056 = database.table("EXT056")
        .index("00")
        .selection(
          "EXBJNO",
          "EXCONO",
          "EXUCA4",
          "EXUCA5",
          "EXUCA6",
          "EXETAD",
          "EXETDD",
          "EXCCUD",
          "EXCONN",
          "EXDLIX",
          "EXRGDT",
          "EXRGTM",
          "EXLMDT",
          "EXCHNO",
          "EXCHID"
        )
        .build()

    DBContainer ListContainerEXT056 = ListqueryEXT056.getContainer()
    ListContainerEXT056.set("EXBJNO", jobNumber)

    //Record exists
    if (!ListqueryEXT056.readAll(ListContainerEXT056, 1, outData)){

    }

    // delete workfile
    DBAction DelQuery = database.table("EXT056").index("00").build()
    DBContainer DelcontainerEXT056 = DelQuery.getContainer()
    DelcontainerEXT056.set("EXBJNO", jobNumber)
    if(!DelQuery.readAllLock(DelcontainerEXT056, 1, deleteCallBack)){
    }
  }

  // liste MHDISH
  Closure<?> MHDISHData = { DBContainer containerMHDISH ->

    sameUDE1 = false
    sameCCUD = false
    dlix_MHDISH = containerMHDISH.get("OQDLIX")
    conn_MHDISH = containerMHDISH.get("OQCONN")
    logger.debug("DLIX : " + dlix_MHDISH)


    found_DRADTR = true
    index = 0
    shipment = 0
    dossier = ""
    semaine = ""
    annee = ""
    arrivalDate = 0
    departureDate = 0
    packagindDate = 0
    
    DBAction query_DRADTR = database.table("DRADTR").index("00").selection("DRDLIX","DRCONN","DRETAD","DRETDD","DRCCUD","DRUDE1","DRUDE2","DRUDE3").build()
    DBContainer DRADTR = query_DRADTR.getContainer()
    DRADTR.set("DRCONO", currentCompany)
    DRADTR.set("DRTLVL", 1)
    DRADTR.set("DRCONN", conn_MHDISH)
    //DRADTR.set("DRDLIX", dlix_MHDISH)
    if(!query_DRADTR.readAll(DRADTR, 3, DRADTRData)){
      found_DRADTR = false
    }
		
		

    if (mi.in.get("UDE1") != null) {
      if (ude1_input.trim() == dossier.trim()) {
        sameUDE1 = true
      }
    } else {
      sameUDE1 = true
    }

    if (mi.in.get("CCUD") != null) {
      if (ccud_input == packagindDate) {
        sameCCUD = true
      }
    } else {
      sameCCUD = true
    }

    if (found_DRADTR && sameUDE1 && sameCCUD) {
       logger.debug("has DLIX : " + dlix_MHDISH)
      //Check if record exists
      DBAction queryEXT056 = database.table("EXT056")
          .index("00")
          .selection(
          "EXBJNO",
          "EXCONO",
          "EXUCA4",
          "EXUCA5",
          "EXUCA6",
          "EXETAD",
          "EXETDD",
          "EXCCUD",
          "EXCONN",
          "EXDLIX",
          "EXRGDT",
          "EXRGTM",
          "EXLMDT",
          "EXCHNO",
          "EXCHID"
          )
          .build()

      DBContainer containerEXT056 = queryEXT056.getContainer()
      containerEXT056.set("EXBJNO", jobNumber)
      containerEXT056.set("EXCONO", currentCompany)
      containerEXT056.set("EXUCA4", dossier)
      containerEXT056.set("EXUCA5", semaine)
      containerEXT056.set("EXUCA6", annee)
      containerEXT056.set("EXDLIX", dlix_MHDISH)
      containerEXT056.set("EXCONN", conn_MHDISH)

      //Record exists
      if (queryEXT056.read(containerEXT056)) {
        Closure<?> updateEXT056 = { LockedResult lockedResultEXT056 ->
          lockedResultEXT056.set("EXETAD", arrivalDate)
          lockedResultEXT056.set("EXETDD", departureDate)
          lockedResultEXT056.set("EXCCUD", packagindDate)
          lockedResultEXT056.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
          lockedResultEXT056.setInt("EXCHNO", ((Integer)lockedResultEXT056.get("EXCHNO") + 1))
          lockedResultEXT056.set("EXCHID", program.getUser())
          lockedResultEXT056.update()
        }
        queryEXT056.readLock(containerEXT056, updateEXT056)
      } else {
        containerEXT056.set("EXBJNO", jobNumber)
        containerEXT056.set("EXCONO", currentCompany)
        containerEXT056.set("EXUCA4", dossier)
        containerEXT056.set("EXUCA5", semaine)
        containerEXT056.set("EXUCA6", annee)
        containerEXT056.set("EXDLIX", dlix_MHDISH)
        containerEXT056.set("EXCONN", conn_MHDISH)
        containerEXT056.set("EXETAD", arrivalDate)
        containerEXT056.set("EXETDD", departureDate)
        containerEXT056.set("EXCCUD", packagindDate)
        containerEXT056.set("EXRGDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT056.set("EXRGTM", utility.call("DateUtil", "currentTimeAsInt"))
        containerEXT056.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
        containerEXT056.set("EXCHNO", 1)
        containerEXT056.set("EXCHID", program.getUser())
        queryEXT056.insert(containerEXT056)
      }
    }
  }

  // data OOLINE
  Closure<?> OOLINEData = { DBContainer ContainerOOLINE ->
    whlo_OOLINE = ContainerOOLINE.get("OBWHLO")
    return
  }
  
    // data DRADTR
  Closure<?> DRADTRData = { DBContainer ContaineDRADTR ->
    arrivalDate = ContaineDRADTR.getInt("DRETAD")
    departureDate = ContaineDRADTR.getInt("DRETDD")
    //packagindDate = ContaineDRADTR.getInt("DRCCUD")
    dossier = ContaineDRADTR.get("DRUDE1")
    semaine = ContaineDRADTR.get("DRUDE2")
    annee = ContaineDRADTR.get("DRUDE3")
    logger.debug("dossier : " + dossier)
    logger.debug("semaine : " + semaine)
    logger.debug("annee : " + annee)
	
	DBAction query_DRADTR_Liv = database.table("DRADTR").index("00").selection("DRDLIX","DRCCUD").build()
		DBContainer DRADTR_Liv = query_DRADTR_Liv.getContainer()
		DRADTR_Liv.set("DRCONO", currentCompany)
		DRADTR_Liv.set("DRTLVL", 2)
		DRADTR_Liv.set("DRCONN", 0)
		logger.debug("DRADTR_Liv dlix_MHDISH : " + dlix_MHDISH)
		DRADTR_Liv.set("DRDLIX", dlix_MHDISH)
		
		if(query_DRADTR_Liv.read(DRADTR_Liv)){
		  packagindDate = DRADTR_Liv.getInt("DRCCUD")
	
	    ship = DRADTR_Liv.get("DRDLIX")
	    logger.debug("Empotage : " + packagindDate + " pour DLIX : " + ship)
		}
    return
  }


  Closure<?> outData = { DBContainer containerEXT056 ->
    String dossierEXT056 = containerEXT056.get("EXUCA4")
    String semaineEXT056 = containerEXT056.get("EXUCA5")
    String anneeEXT056 = containerEXT056.get("EXUCA6")
    String indexEXT056 = containerEXT056.getLong("EXDLIX")
    String shipmentEXT056 = containerEXT056.get("EXCONN")
    String arrivalDateEXT056 = containerEXT056.get("EXETAD")
    String departureDateEXT056 = containerEXT056.get("EXETDD")
    String packagindDateEXT056 = containerEXT056.get("EXCCUD")
    mi.outData.put("UDE1", dossierEXT056)
    mi.outData.put("UDE2", semaineEXT056)
    mi.outData.put("UDE3", anneeEXT056)
    mi.outData.put("ETDD", departureDateEXT056)
    mi.outData.put("ETAD", arrivalDateEXT056)
    mi.outData.put("DLIX", indexEXT056)
    mi.outData.put("CONN", shipmentEXT056)
    mi.outData.put("CCUD", packagindDateEXT056)
    mi.write()
  }

  Closure<?> deleteCallBack = { LockedResult lockedResult ->
    lockedResult.delete()
  }
}

