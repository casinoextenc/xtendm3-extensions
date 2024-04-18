/**
 * README
 * This extension is used by MEC
 *
 * Name : EXT099MI.GetPOLineSIGMA6
 * Description : INT-LOG1203 find OOLINE by POPN and CUNO
 * Date         Changed By    Description
 * 20230822     FLEBARS       LOG1203 Creation
 */
public class PPS912retrieveSupplier extends ExtendM3Trigger {
  private final ProgramAPI program;
  private final DatabaseAPI database
  private final LoggerAPI logger
  private final UtilityAPI utility
  private final MICallerAPI miCaller
  private final SessionAPI session
  private final MethodAPI method;

  public PPS912retrieveSupplier(MethodAPI method, SessionAPI session, ProgramAPI program, DatabaseAPI database, LoggerAPI logger, UtilityAPI utility, MICallerAPI miCaller) {
    this.session = session
    this.database = database
    this.logger = logger
    this.program = program
    this.utility = utility
    this.miCaller = miCaller
    this.method = method
  }

  public void main() {
    Map<String, Object> mm = session.getParameters()
    logger.debug("SESSION BANZAI")
    for (Map.Entry<String, Object> entry : mm.entrySet()){
      logger.debug("SESSION " + entry.getKey() + ":" + entry.getValue())
    }
    logger.debug("LA PLAT")
  }
}
