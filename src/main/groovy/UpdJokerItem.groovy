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

  private String bjnoInput
  private String itnoInput
  private double zquvInput
  private double zpqaInput
  private double mitaunCofa
  private int dmcf
  private String mitmasPuun
  private double palQuantity
  private double totQuantity
  private double totWeight
  private double totVolume
  private double mitmasWeight
  private double mitmasVolume
  private int currentCompany
  private Integer nbMaxRecord = 10000

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

    bjnoInput = (mi.in.get("BJNO") != null ? (String)mi.in.get("BJNO") : "")
    itnoInput = (mi.in.get("ITNO") != null ? (String)mi.in.get("ITNO") : "")
    zquvInput = (double) (mi.in.get("ZQUV") != null ? mi.in.get("ZQUV") : 0)
    zpqaInput = (double) (mi.in.get("ZPQA") != null ? mi.in.get("ZPQA") : 0)

    //Check if record exists in Constraint Type Table (EXT055)
    DBAction query = database.table("EXT055").index("00").build()
    // list out data
    DBAction ListqueryEXT055 = database.table("EXT055").index("00").selection("EXBJNO").build()
    DBContainer ListContainerEXT055 = ListqueryEXT055.getContainer()
    ListContainerEXT055.set("EXBJNO", bjnoInput)
    //Record exists
    if (!ListqueryEXT055.readAll(ListContainerEXT055, 1, nbMaxRecord, existData)){
      mi.error("Numéro de job " + bjnoInput + " n'existe pas dans la table EXT055")
    }

    //Check if Item exist
    mitmasPuun = ""
    DBAction queryMitmas = database.table("MITMAS").index("00").selection("MMPUUN","MMVOL3","MMGRWE","MMSUNO").build()
    DBContainer MITMAS = queryMitmas.getContainer()
    MITMAS.set("MMCONO", currentCompany)
    MITMAS.set("MMITNO", itnoInput)
    if(!queryMitmas.read(MITMAS)){
      mi.error("Code article " + itnoInput + " n'existe pas")
    } else {
      mitmasPuun = MITMAS.get("MMPUUN")
      mitmasVolume = MITMAS.getDouble("MMVOL3")
      mitmasWeight = MITMAS.getDouble("MMGRWE")
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
    containerEXT055.set("EXBJNO", bjnoInput)
    containerEXT055.set("EXCONO", currentCompany)
    containerEXT055.set("EXITNO", itnoInput)
    if(!queryEXT055.readLock(containerEXT055, updateCallBack)){
      mi.error("L'enregistrement n'existe pas")
      return
    }
  }

  /**
   * Update EXT055
   */
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
    palQuantity = zpqaInput
    totQuantity = 0
    DBContainer containerMITAUN = queryMITAUN00.getContainer()
    containerMITAUN.set("MUCONO", currentCompany)
    containerMITAUN.set("MUITNO", itnoInput)
    containerMITAUN.set("MUAUTP", 1)
    containerMITAUN.set("MUALUN", mitmasPuun)
    if (queryMITAUN00.read(containerMITAUN)) {
      mitaunCofa = containerMITAUN.getDouble("MUCOFA")
      dmcf = containerMITAUN.getInt("MUDMCF")
      if (dmcf == 1) {
        palQuantity = zpqaInput * mitaunCofa
      } else {
        palQuantity = zpqaInput / mitaunCofa
      }
    }

    totQuantity = palQuantity + zpqaInput
    totWeight = totQuantity * mitmasWeight
    totVolume = totQuantity * mitmasVolume
    logger.debug("totQuantity : " + totQuantity)
    logger.debug("totWeight : " + totWeight)
    logger.debug("totVolume : " + totVolume)
    logger.debug("mitaunCofa : " + mitaunCofa)
    logger.debug("dmcf : " + dmcf)

    lockedResultEXT055.set("EXZQUV", zquvInput)
    lockedResultEXT055.set("EXZPQA", palQuantity)
    lockedResultEXT055.set("EXGRWE", totWeight)
    lockedResultEXT055.set("EXVOL3", totVolume)
    lockedResultEXT055.set("EXLMDT", utility.call("DateUtil", "currentDateY8AsInt"))
    lockedResultEXT055.setInt("EXCHNO", ((Integer)lockedResultEXT055.get("EXCHNO") + 1))
    lockedResultEXT055.set("EXCHID", program.getUser())
    lockedResultEXT055.update()
  }

  /**
   * Get EXT055 data
   */
  Closure<?> existData = { DBContainer containerEXT055 ->
    return
  }

}
