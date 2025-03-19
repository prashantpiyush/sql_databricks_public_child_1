from dataclasses import dataclass

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class child_deduplicate_custom(MacroSpec):
    name: str = "child_deduplicate_custom"
    projectName: str = "SQL_DatabricksSharedBasic"
    category: str = "Custom"


    @dataclass(frozen=True)
    class deduplicateProperties(MacroProperties):
        # properties for the component with default values
        relation: str = ''
        order_by: str = ''
        partition_by: str = ''
        

    def dialog(self) -> Dialog:
        return Dialog("Macro").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=True),
                "content"
            )
            .addColumn(
                StackLayout()
                .addElement(
                    TextBox("Table Name")
                    .withHelpText("Configure table name to deduplicate")
                    .bindProperty("relation")
                )
                .addElement(
                    TextBox("Order By")
                    .withHelpText("Determines the priority order of which row should be chosen if there are duplicates")
                    .bindProperty("order_by")
                )
                .addElement(
                    TextBox("Partition By")
                    .withHelpText("Used to identify a set/window of rows out of which to select one as the deduplicated row")
                    .bindProperty("partition_by")
                )
           )
       )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return []

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        return newState

    def apply(self, props: deduplicateProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        non_empty_param = ",".join([f"'{param}'" for param in [props.relation, props.order_by, props.partition_by] if param != ''])
        return f'{{{{ {resolved_macro_name}({non_empty_param}) }}}}'

    def _strip(self, relation):
        if relation.startswith("'"):
            relation = relation[1:]
        if relation.endswith("'"):
            relation = relation[:-1]
        return relation     

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)    
        return child_deduplicate_custom.deduplicateProperties(
            relation=self._strip(parametersMap.get('relation')),
            order_by=self._strip(parametersMap.get('order_by')),
            partition_by=self._strip(parametersMap.get('partition_by')),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation", f"'{properties.relation}'"),
                MacroParameter("order_by", f"'{properties.order_by}'"),
                MacroParameter("partition_by", f"'{properties.partition_by}'"),
            ],
        )