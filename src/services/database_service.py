import logging
from typing import List, Dict, Any, Optional
import asyncpg
from config.settings import settings
from models.omop_models import OMOPConcept, ConceptMapping

logger = logging.getLogger(__name__)


class DatabaseService:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def get_connection_pool(self) -> asyncpg.Pool:
        """Get or create database connection pool."""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                user=settings.database_user,
                password=settings.database_password,
                database=settings.database_name,
                host=settings.database_endpoint,
                port=5432,
                min_size=1,
                max_size=10
            )
        return self.pool
    
    async def close_pool(self):
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None
    
    async def execute_verbatim_query(
        self, 
        concepts: List[ConceptMapping], 
        schema: str
    ) -> List[OMOPConcept]:
        """Execute verbatim matching query (exact concept_code and vocabulary_id)."""
        pool = await self.get_connection_pool()
        
        query = f"""
        SELECT c.concept_id, c.concept_code, c.vocabulary_id,
               c.domain_id, c.concept_class_id, c.concept_name,
               $1 as concept_set_id, $2 as concept_set_name,
               $3 as source_vocabulary
        FROM {schema}.concept c 
        WHERE c.concept_code = $4 AND c.vocabulary_id = $5
        ORDER BY c.concept_id
        """
        
        results = []
        async with pool.acquire() as conn:
            for concept in concepts:
                try:
                    rows = await conn.fetch(
                        query,
                        concept.concept_set_id,
                        concept.concept_set_name,
                        concept.original_vocabulary,
                        concept.concept_code,
                        concept.vocabulary_id
                    )
                    
                    for row in rows:
                        results.append(OMOPConcept(
                            concept_id=row['concept_id'],
                            concept_code=row['concept_code'],
                            vocabulary_id=row['vocabulary_id'],
                            domain_id=row['domain_id'],
                            concept_class_id=row['concept_class_id'],
                            concept_name=row['concept_name'],
                            mapping_type='verbatim'
                        ))
                except Exception as error:
                    logger.error(f"Error in verbatim query for {concept.concept_code}: {error}")
                    continue
        
        return results
    
    async def execute_standard_query(
        self, 
        concepts: List[ConceptMapping], 
        schema: str
    ) -> List[OMOPConcept]:
        """Execute standard concept matching query (standard_concept = 'S')."""
        pool = await self.get_connection_pool()
        
        query = f"""
        SELECT c.concept_id, c.concept_code, c.vocabulary_id,
               c.domain_id, c.concept_class_id, c.concept_name,
               c.standard_concept, $1 as concept_set_id, $2 as concept_set_name,
               $3 as source_vocabulary
        FROM {schema}.concept c 
        WHERE c.concept_code = $4 AND c.vocabulary_id = $5
        AND c.standard_concept = 'S'
        ORDER BY c.concept_id
        """
        
        results = []
        async with pool.acquire() as conn:
            for concept in concepts:
                try:
                    rows = await conn.fetch(
                        query,
                        concept.concept_set_id,
                        concept.concept_set_name,
                        concept.original_vocabulary,
                        concept.concept_code,
                        concept.vocabulary_id
                    )
                    
                    for row in rows:
                        results.append(OMOPConcept(
                            concept_id=row['concept_id'],
                            concept_code=row['concept_code'],
                            vocabulary_id=row['vocabulary_id'],
                            domain_id=row['domain_id'],
                            concept_class_id=row['concept_class_id'],
                            concept_name=row['concept_name'],
                            standard_concept=row['standard_concept'],
                            mapping_type='standard'
                        ))
                except Exception as error:
                    logger.error(f"Error in standard query for {concept.concept_code}: {error}")
                    continue
        
        return results
    
    async def execute_mapped_query(
        self, 
        concepts: List[ConceptMapping], 
        schema: str
    ) -> List[OMOPConcept]:
        """Execute mapped concept query (via 'Maps to' relationships)."""
        pool = await self.get_connection_pool()
        
        query = f"""
        SELECT cr.concept_id_2 AS concept_id, c.concept_code, c.vocabulary_id,
               c.concept_id as source_concept_id, cr.relationship_id,
               target_c.concept_name, target_c.domain_id, target_c.concept_class_id,
               target_c.standard_concept, $1 as concept_set_id, $2 as concept_set_name,
               $3 as source_vocabulary
        FROM {schema}.concept c 
        INNER JOIN {schema}.concept_relationship cr
        ON c.concept_id = cr.concept_id_1
        AND cr.relationship_id = 'Maps to'
        INNER JOIN {schema}.concept target_c
        ON cr.concept_id_2 = target_c.concept_id
        WHERE c.concept_code = $4 AND c.vocabulary_id = $5
        ORDER BY cr.concept_id_2
        """
        
        results = []
        async with pool.acquire() as conn:
            for concept in concepts:
                try:
                    rows = await conn.fetch(
                        query,
                        concept.concept_set_id,
                        concept.concept_set_name,
                        concept.original_vocabulary,
                        concept.concept_code,
                        concept.vocabulary_id
                    )
                    
                    for row in rows:
                        results.append(OMOPConcept(
                            concept_id=row['concept_id'],
                            source_concept_id=row['source_concept_id'],
                            concept_code=row['concept_code'],
                            vocabulary_id=row['vocabulary_id'],
                            domain_id=row['domain_id'],
                            concept_class_id=row['concept_class_id'],
                            concept_name=row['concept_name'],
                            standard_concept=row['standard_concept'],
                            relationship_id=row['relationship_id'],
                            mapping_type='mapped'
                        ))
                except Exception as error:
                    logger.error(f"Error in mapped query for {concept.concept_code}: {error}")
                    continue
        
        return results


# Singleton instance
database_service = DatabaseService()