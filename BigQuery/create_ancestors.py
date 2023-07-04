from dataclasses import field, dataclass
import base_table_creator
@dataclass
class CreateSnapRelationship(base_table_creator.BaseTableCreator):
    table: str = "SnapRelationship"
    
    @property
    def sql(self) -> str:
        return f"""
        SELECT * FROM `{self.dataset}.Relationship` r
        WHERE r.active=True AND r.effectiveTime=(SELECT MAX(effectiveTime) FROM `{self.dataset}.Relationship` WHERE id=r.id)
        """

@dataclass
class CreateSnapIsARelationship(base_table_creator.BaseTableCreator):
    table: str = "SnapIsARelationship"
    
    @property
    def sql(self) -> str:
        return f"""
        SELECT * FROM `{self.dataset}.SnapRelationship` r
        WHERE r.typeId = 116680003
        """

@dataclass
class CreateTransitiveClosure(base_table_creator.BaseTableCreator):
    table: str = "TransitiveClosure"
        
    @property
    def sql(self) -> str:
        return f"""
        WITH RECURSIVE child AS (
        # Initial table to start. We only need sourceId and destinationId 
        # and keep a counter for the levels we've traversed
            (SELECT 
                sourceId,
                destinationId,
                1 AS descendantLevel
            FROM `{self.dataset}.SnapIsARelationship`
            )
        
        UNION ALL # Concat all tables we generate vertically
        
            # Create a new table by joining relations adn matching our previous (child) destinationId with the next (parent) sourceId
            # We keep the original sourceId but adopt the next destinationId for next iteration, and increase our level counter.
            SELECT 
                c.sourceId as sourceId,
                parent.destinationId as destinationId,
                descendantLevel+1 AS descendantLevel
            FROM `{self.dataset}.SnapIsARelationship` parent
            JOIN child c 
            ON c.destinationId = parent.sourceId
            WHERE descendantLevel <= 50 # go max 50 levels deep in the hierarchy
        
        )
        
        SELECT DISTINCT * 
        FROM child c
        ORDER BY sourceId,descendantLevel;
        """

@dataclass
class CreateAncestors(base_table_creator.BaseTableCreator):
    table: str = "Ancestors"
        
    @property
    def sql(self) -> str:
        return f"""
        SELECT sourceId as conceptId, array_agg(destinationId) as ancestorIds
        FROM `{self.dataset}.TransitiveClosure`
        GROUP BY sourceId
        """
@dataclass
class CreateSnapLanguageRefSet(base_table_creator.BaseTableCreator):
    table:str = field(default="SnapLanguageRefSet")

    @property
    def sql(self) -> str:
        return f"""
        SELECT * FROM `{self.dataset}.LanguageRefSet` l
        WHERE l.active=true AND l.effectiveTime=(SELECT MAX(effectiveTime) FROM `{self.dataset}.LanguageRefSet` WHERE id=l.id)
        """

@dataclass
class CreateSnapConcept(base_table_creator.BaseTableCreator):
    table: str = field(default="SnapConcept")
        
    @property
    def sql(self) -> str:
        return f"""
        SELECT * FROM `{self.dataset}.Concept` c 
        WHERE c.active=True AND c.effectiveTime=(SELECT MAX(effectiveTime) FROM `{self.dataset}.Concept` WHERE id=c.id)
        """

@dataclass
class CreateSnapDescription(base_table_creator.BaseTableCreator):
    table: str = "SnapDescription"

    @property
    def sql(self) -> str:
        return f"""
        SELECT * FROM `{self.dataset}.Description` d 
        WHERE d.active=True AND d.effectiveTime=(SELECT MAX(effectiveTime) FROM `{self.dataset}.Description` WHERE id=d.id)
        """

@dataclass
class CreateHSnapDescription(base_table_creator.BaseTableCreator):
    table: str = "HSnapDescription"

    @property
    def sql(self) -> str:
        return f"""
        SELECT d.*, td.term as typeTerm, cd.term as caseSignificanceTerm, md.term as moduleTerm FROM `{self.dataset}.SnapDescription` d
        LEFT JOIN `{self.dataset}.SnapDescription` td 
        ON d.typeId = td.conceptId 
        AND td.typeId = 900000000000003001 # Fully Specified Name
        AND td.moduleId = 900000000000012004 # SNOMED-CT model component
        LEFT JOIN `{self.dataset}.SnapDescription` cd 
        ON d.caseSignificanceId = cd.conceptId
        AND cd.typeId = 900000000000003001 # Fully Specified Name
        AND cd.moduleId = 900000000000012004 # SNOMED-CT model component
        LEFT JOIN `{self.dataset}.SnapDescription` md 
        ON d.moduleId = md.conceptId 
        AND md.typeId = 900000000000003001 # Fully Specified Name
        AND md.languageCode = "en"
        """

@dataclass
class CreateInternationalFSNSnapDescription(base_table_creator.BaseTableCreator):
    table: str = "InternationalFSNSnapDescription"

    @property
    def sql(self) -> str:
        return f"""
        SELECT * FROM `{self.dataset}.HSnapDescription`
        WHERE typeId = 900000000000003001 # Fully specified name
        AND moduleId = 900000000000207008 # SNOMED-CT core module
        """