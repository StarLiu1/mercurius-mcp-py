"""
Tool 7: Complete CQL to SQL translation - orchestrates Tools 1-6.
"""
import asyncio
from datetime import datetime
import logging
from typing import Dict, Any, Optional
from mcp.server.fastmcp import Context

from tools.parse_cql_structure import parse_cql_structure_tool
from tools.extract_valuesets_with_omop import extract_valuesets_with_omop_tool
from tools.generate_omop_sql import generate_omop_sql_tool
from tools.validate_generated_sql import validate_generated_sql_tool
from tools.correct_sql_errors import correct_sql_errors_tool
from tools.finalize_sql import finalize_sql_tool

logger = logging.getLogger(__name__)


async def translate_cql_to_sql_complete_tool(
    cql_content: str,
    ctx: Context,
    cql_file_path: Optional[str] = None,
    sql_dialect: str = "postgresql",
    validate: bool = True,
    correct_errors: bool = True,
    config: Dict[str, Any] = None,
    vsac_username: Optional[str] = None,
    vsac_password: Optional[str] = None,
    database_user: Optional[str] = None,
    database_endpoint: Optional[str] = None,
    database_name: Optional[str] = None,
    database_password: Optional[str] = None,
    omop_database_schema: Optional[str] = None
) -> Dict[str, Any]:
    """
    Tool 7: Complete CQL to SQL translation pipeline (orchestrates Tools 1-6).
    
    Full 6-step LLM-driven workflow:
    1. Parse CQL structure and analyze dependencies (LLM)
    2. Extract valuesets and map to OMOP (MCP tool)
    3. Generate SQL with placeholders (LLM)
    4. Validate SQL semantically and syntactically (LLM)
    5. Correct SQL errors if found (LLM)
    6. Replace placeholders with concept IDs (Programmatic)
    
    Args:
        cql_content: CQL content to translate
        cql_file_path: Path to CQL file (for finding libraries)
        sql_dialect: Target SQL dialect (postgresql, snowflake, bigquery, sqlserver)
        validate: Whether to run validation (Tool 4)
        correct_errors: Whether to correct errors if validation fails (Tool 5)
        config: Path to config.yaml
        vsac_username: VSAC username (optional, uses env var)
        vsac_password: VSAC password (optional, uses env var)
        database_*: Database config (optional, uses env vars)
        
    Returns:
        Dict with:
        - success: Overall pipeline success
        - final_sql: Complete SQL query ready to run
        - pipeline_results: Results from each tool
        - statistics: Comprehensive statistics
        - errors: Any errors encountered
    """
    start_time = datetime.now()

    async def send_heartbeat():
        """Send progress every 15 seconds"""
        while True:
            await asyncio.sleep(15)
            elapsed = (datetime.now() - start_time).total_seconds()
            await ctx.report_progress(
                progress=int(elapsed),
                total=None,  # Unknown total is fine!
                message=f"Still working... ({elapsed:.0f}s elapsed)"
            )
    
    # Start heartbeat task
    heartbeat_task = asyncio.create_task(send_heartbeat())

    try:
        logger.info("=" * 80)
        logger.info("COMPLETE CQL TO SQL TRANSLATION PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Target SQL dialect: {sql_dialect}")
        logger.info(f"Validation enabled: {validate}")
        logger.info(f"Error correction enabled: {correct_errors}")
        logger.info("=" * 80)
        
        pipeline_results = {}
        errors = []
        
        # ====================================================================
        # TOOL 1: Parse CQL Structure
        # ====================================================================
        logger.info("\n🔹 Step 1/6: Parsing CQL structure...")
        
        tool1_result = await parse_cql_structure_tool(
            cql_content=cql_content,
            cql_file_path=cql_file_path,
            config=config
        )
        
        pipeline_results['tool1_parse'] = tool1_result
        
        if not tool1_result.get('success'):
            logger.error(f"❌ Tool 1 failed: {tool1_result.get('error')}")
            errors.append(f"Parse failed: {tool1_result.get('error')}")
            return {
                "success": False,
                "pipeline_results": pipeline_results,
                "errors": errors,
                "failed_at": "tool1_parse_cql_structure"
            }
        
        logger.info(f"✅ Tool 1 complete: {tool1_result['statistics']['definitions_count']} definitions")
        
        # ====================================================================
        # TOOL 2: Extract Valuesets and Map to OMOP
        # ====================================================================
        logger.info("\n🔹 Step 2/6: Extracting valuesets and mapping to OMOP...")
        
        tool2_result = await extract_valuesets_with_omop_tool(
            cql_content=cql_content,
            library_files=tool1_result.get('library_files'),
            parsed_structure=tool1_result.get('parsed_structure'),
            library_definitions=tool1_result.get('library_definitions'),
            vsac_username=vsac_username,
            vsac_password=vsac_password,
            database_user=database_user,
            database_endpoint=database_endpoint,
            database_name=database_name,
            database_password=database_password,
            omop_database_schema=omop_database_schema
        )
        
        pipeline_results['tool2_extract'] = tool2_result
        
        if not tool2_result.get('success'):
            logger.error(f"❌ Tool 2 failed: {tool2_result.get('error')}")
            errors.append(f"Extraction failed: {tool2_result.get('error')}")
            return {
                "success": False,
                "pipeline_results": pipeline_results,
                "errors": errors,
                "failed_at": "tool2_extract_valuesets_with_omop"
            }
        
        logger.info(f"✅ Tool 2 complete: {tool2_result['statistics']['total_valuesets_extracted']} valuesets")
        
        # ====================================================================
        # TOOL 3: Generate SQL
        # ====================================================================
        logger.info("\n🔹 Step 3/6: Generating SQL...")
        
        tool3_result = await generate_omop_sql_tool(
            parsed_structure=tool1_result['parsed_structure'],
            all_valuesets=tool2_result['all_valuesets'],
            cql_content=cql_content,
            placeholder_mappings=tool2_result['placeholder_mappings'],
            dependency_analysis=tool1_result.get('dependency_analysis'),
            library_definitions=tool1_result.get('library_definitions'),
            valueset_registry=tool2_result.get('valueset_registry'),
            individual_codes=tool2_result.get('individual_codes'),
            sql_dialect=sql_dialect,
            config=config
        )
        
        pipeline_results['tool3_generate'] = tool3_result
        
        if not tool3_result.get('success'):
            logger.error(f"❌ Tool 3 failed: {tool3_result.get('error')}")
            errors.append(f"SQL generation failed: {tool3_result.get('error')}")
            return {
                "success": False,
                "pipeline_results": pipeline_results,
                "errors": errors,
                "failed_at": "tool3_generate_omop_sql"
            }
        
        logger.info(f"✅ Tool 3 complete: {len(tool3_result['sql']):,} characters")
        
        # Get SQL for validation/correction
        current_sql = tool3_result['sql']
        
        # ====================================================================
        # TOOL 4: Validate SQL (Optional)
        # ====================================================================
        validation_result = None
        if validate:
            logger.info("\n🔹 Step 4/6: Validating SQL...")
            
            tool4_result = await validate_generated_sql_tool(
                sql_query=current_sql,
                parsed_structure=tool1_result['parsed_structure'],
                all_valuesets=tool2_result['all_valuesets'],
                sql_dialect=sql_dialect,
                config=config
            )
            
            pipeline_results['tool4_validate'] = tool4_result
            validation_result = tool4_result
            
            if not tool4_result.get('success'):
                logger.warning(f"⚠️ Tool 4 had issues: {tool4_result.get('error')}")
                errors.append(f"Validation had issues: {tool4_result.get('error')}")
            else:
                if tool4_result.get('valid'):
                    logger.info(f"✅ Tool 4 complete: SQL is valid")
                else:
                    logger.warning(f"⚠️ Tool 4 complete: {tool4_result['issue_counts']['errors']} errors found")
        else:
            logger.info("\n⏭️ Step 4/6: Validation skipped (validate=False)")
        
        # ====================================================================
        # TOOL 5: Correct SQL Errors (Optional)
        # ====================================================================
        if correct_errors and validation_result and not validation_result.get('valid'):
            logger.info("\n🔹 Step 5/6: Correcting SQL errors...")
            
            tool5_result = await correct_sql_errors_tool(
                sql_query=current_sql,
                validation_result=validation_result,
                parsed_structure=tool1_result['parsed_structure'],
                sql_dialect=sql_dialect,
                config=config
            )
            
            pipeline_results['tool5_correct'] = tool5_result
            
            if tool5_result.get('success') and tool5_result.get('sql_changed'):
                current_sql = tool5_result['corrected_sql']
                logger.info(f"✅ Tool 5 complete: {len(tool5_result['changes_made'])} corrections made")
            elif tool5_result.get('success'):
                logger.info(f"✅ Tool 5 complete: No corrections needed")
            else:
                logger.error(f"❌ Tool 5 failed: {tool5_result.get('error')}")
                errors.append(f"SQL correction failed: {tool5_result.get('error')}")
        else:
            if not correct_errors:
                logger.info("\n⏭️ Step 5/6: Error correction skipped (correct_errors=False)")
            elif not validation_result:
                logger.info("\n⏭️ Step 5/6: Error correction skipped (no validation)")
            else:
                logger.info("\n⏭️ Step 5/6: Error correction skipped (SQL is valid)")
        
        # ====================================================================
        # TOOL 6: Replace Placeholders
        # ====================================================================
        logger.info("\n🔹 Step 6/6: Replacing placeholders with concept IDs...")
        
        tool6_result = await finalize_sql_tool(
            sql_query=current_sql,
            placeholder_mappings=tool2_result['placeholder_mappings'],
            sql_dialect=sql_dialect
        )
        
        pipeline_results['tool6_finalize'] = tool6_result
        
        if not tool6_result.get('success'):
            logger.error(f"❌ Tool 6 failed: {tool6_result.get('error')}")
            errors.append(f"Placeholder replacement failed: {tool6_result.get('error')}")
            # Still return SQL even if some placeholders unmapped
        
        logger.info(f"✅ Tool 6 complete: {tool6_result['statistics']['replacements_made']} placeholders replaced")
        
        # ====================================================================
        # Compile Final Results
        # ====================================================================
        final_sql = tool6_result.get('final_sql', current_sql)
        
        # Comprehensive statistics
        comprehensive_stats = {
            "pipeline": {
                "libraries_processed": tool1_result['statistics']['library_files_found'],
                "definitions_parsed": tool1_result['statistics']['definitions_count'],
                "valuesets_extracted": tool2_result['statistics']['total_valuesets_extracted'],
                "individual_codes": tool2_result['statistics']['total_individual_codes'],
                "omop_concepts_mapped": tool2_result['statistics']['total_concept_ids'],
                "ctes_generated": tool3_result['statistics']['cte_count'],
                "placeholders_found": tool6_result['statistics']['placeholders_found'],
                "placeholders_replaced": tool6_result['statistics']['placeholders_replaced'],
                "unmapped_placeholders": tool6_result['statistics']['unmapped_placeholders']
            },
            "validation": {
                "enabled": validate,
                "passed": validation_result.get('valid') if validation_result else None,
                "errors": validation_result['issue_counts']['errors'] if validation_result else 0,
                "warnings": validation_result['issue_counts']['warnings'] if validation_result else 0
            } if validate else None,
            "correction": {
                "enabled": correct_errors,
                "changes_made": len(pipeline_results.get('tool5_correct', {}).get('changes_made', []))
            } if correct_errors else None,
            "sql": {
                "dialect": sql_dialect,
                "final_length": len(final_sql),
                "ready_to_execute": tool6_result.get('success', False)
            }
        }
        
        overall_success = (
            tool1_result.get('success') and
            tool2_result.get('success') and
            tool3_result.get('success') and
            tool6_result.get('success')
        )
        
        logger.info("\n" + "=" * 80)
        if overall_success:
            logger.info("✅ PIPELINE COMPLETE: CQL Successfully Translated to SQL")
        else:
            logger.error("❌ PIPELINE FAILED: See errors for details")
        logger.info("=" * 80)
        logger.info(f"📊 Statistics:")
        logger.info(f"   - Valuesets: {comprehensive_stats['pipeline']['valuesets_extracted']}")
        logger.info(f"   - OMOP Concepts: {comprehensive_stats['pipeline']['omop_concepts_mapped']}")
        logger.info(f"   - SQL Length: {comprehensive_stats['sql']['final_length']:,} characters")
        logger.info(f"   - Placeholders: {comprehensive_stats['pipeline']['placeholders_replaced']}/{comprehensive_stats['pipeline']['placeholders_found']}")
        if validate:
            logger.info(f"   - Validation: {'✅ PASSED' if comprehensive_stats['validation']['passed'] else '❌ FAILED'}")
        logger.info("=" * 80)
        
        return {
            "success": overall_success,
            "final_sql": final_sql,
            "sql_dialect": sql_dialect,
            "pipeline_results": pipeline_results,
            "statistics": comprehensive_stats,
            "errors": errors if errors else None
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed with exception: {e}", exc_info=True)
        return {
            "success": False,
            "error": str(e),
            "pipeline_results": pipeline_results if 'pipeline_results' in locals() else {},
            "errors": errors if 'errors' in locals() else [str(e)]
        }

    finally:
        # Stop heartbeat when done
        heartbeat_task.cancel()