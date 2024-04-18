/**
 * README
 * This extension is used by Mashup
 * 
 * Name : EXT050MI.UpdJokerItem
 * Description : Update Joker item
 * Date         Changed By   Description
 * 20210125     SEAR         QUAX01 - Constraints matrix 
 */
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.math.RoundingMode

public class UpdJokerItem extends ExtendM3Transaction {
  private final MIAPI mi
  private final LoggerAPI logger
  private final ProgramAPI program
  private final DatabaseAPI database
  private final SessionAPI session
  private final TransactionAPI transaction
  private final MICallerAPI miCaller
  private final UtilityAPI utility

  private String bjno_input
  private String itno_input
  private double zquv_input
  private double zpqa_input
  private double MITAUN_cofa
  private int dmcf
  private String MITMAS_puun
  private double palQuantity
  private double totQuantity
  private double totWeight
  private double totVolume
  private double MITMAS_Weight
  private double MITMAS_Volume
  private int currentCompany

  public UpdJokerItem(LoggerAPI logger, MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller, UtilityAPI utility) {
    this.logger = logger
    this.mi = mi
    this.database = database
    this.program = program
    this.miCaller = miCaller
    this.utility = utility
  }

  public void main() {

    if (mi.in.get("CONO") == null) {
      currentCompany = (Integer)program.getLDAZD().CONO
    } else {
      currentCompany = mi.in.get("CONO")
    }

    bjno_input = (mi.in.get("BJNO") != null ? (String)mi.in.get("BJNO") : "")
    itno_input = (mi.in.get("ITNO") != null ? (String)mi.in.get("ITNO") : "")
    zquv_input = (double) (mi.in.get("ZQUV") != null ? mi.in.get("ZQUV") : 0)
    zpqa_input = (double) (mi.in.get("ZPQA") != null ? mi.in.get("ZPQA") : 0)

    //Check if record exists in Constraint Type Table (EXT055)
    DBAction query = database.table("EXT055").index("00").build()
    // list out data
    DBAction ListqueryEXT055 = database.table("EXT055").index("00").selection("EXBJNO").build()
    DBContainer ListContainerEXT055 = ListqueryEXT055.getContainer()
    ListContainerEXT055.set("EXBJNO", bjno_input)
    //Record exists
    if (!ListqueryEXT055.readAll(ListContainerEXT055, 1, existData)){
      mi.error("Numéro de job " + bjno_input + " n'existe pas dans la table EXT055")
    }

    //Check if Item exist
    MITMAS_puun = ""
    DBAction query_MITMAS = database.table("MITMAS").index("00").selection("MMPUUN","MMVOL3","MMGRWE","MMSUNO").build()
    DBContainer MITMAS = query_MITMAS.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", itno_input)
    if(!query_MITMAS.read(MITMAS)){
      mi.error("Code article " + itno_input + " n'existe pas")
    } else {
      MITMAS_puun = MITMAS.get("MMPUUN")
      MITMAS_Volume = MITMAS.getDouble("MMVOL3")
      MITMAS_Weight = MITMAS.getDouble("MMGRWE")
    }

    DBAction queryEXT055 = database.table("EXT055")
        .index("00")
        .selection(
        "EXBJNO",
        "EXCONO",
        "EXITNO",
        "EXITDS",
        "EXZAV1",
        "EXZAV2",
        "EXZQUV",
        "EXZPQA",
        "EXGRWE",
        "EXVOL3",
        "EXRGDT",
        "EXRGTM",
        "EXLMDT",
        "EXCHNO",
        "EXCHID"
        )
        .build()

    DBContainer containerEXT055 = queryEXT055.getContainer()
    containerEXT055.set("EXBJNO", bjno_input)
    containerEXT055.set("EXCONO", currentCompany)
    containerEXT055.set("EXITNO", itno_input)
    if(!queryEXT055.readLock(containerEXT055, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  Closure<?> updateCallBack = { LockedResult lockedResultEXT055 ->
    LocalDateTime timeOfCreation = LocalDateTime.now()
    int changeNumber = lockedResultEXT055.get("EXCHNO")

    DBAction queryMITAUN00 = database.table("MITAUN").index("00").selection(
        "MUCONO",
        "MUITNO",
        "MUAUTP",
        "MUALUN",
        "MUCOFA",
        "MUDMCF"
        ).build()

    totWeight = 0
    totVolume = 0
    palQuantity = zpqa_input
    totQuantity = 0
    DBContainer containerMITAUN = queryMITAUN00.getContainer()
    containerMITAUN.set("MUCONO", currentCompany)
    containerMITAUN.set("MUITNO", itno_input)
    containerMITAUN.set("MUAUTP", 1)
    containerMITAUN.set("MUALUN", MITMAS_puun)
    if (queryMITAUN00.read(containerMITAUN)) {
        MITAUN_cofa = containerMITAUN.getDouble("MUCOFA")
        dmcf = containerMITAUN.getInt("MUDMCF")
      if (dmcf == 1) {
        palQuantity = zpqa_input * MITAUN_cofa
      } else {
        palQuantity = zpqa_input / MITAUN_cofa
      }
    }

    totQuantity = palQuantity + zpqa_input
    totWeight = totQuantity * MITMAS_Weight
    totVolume = totQuantity * MITMAS_Volume
    logger.debug("totQuantity : " + totQuantity)
    logger.debug("totWeight : " + totWeight)
    logger.debug("totVolume : " + totVolume)
    logger.debug("MITAUN_cofa : " + MITAUN_cofa)
    logger.debug("dmcf : " + dmcf)

    lockedResultEXT055.set("EXZQUV", zquv_input)
    lockedResultEXT055.set("EXZPQA", palQuantity)
    lockedResultEXT055.set("EXGRWE", totWeight)
    lockedResultEXT055.set("EXVOL3", totVolume)
    lockedResultEXT055.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
    lockedResultEXT055.setInt("EXCHNO", ((Integer)lockedResultEXT055.get("EXCHNO") + 1))
    lockedResultEXT055.set("EXCHID", program.getUser())
    lockedResultEXT055.update()
  }

  Closure<?> existData = { DBContainer containerEXT055 ->
    return
  }

}
