import pandas as pd

class Component:
    
    def __init__(self, name, calculate):
        self.name = name
        accepted_calculations = ['total', 'average', 'dummy', 'sum']
        if calculate in accepted_calculations:
            self.calculate = calculate
        else:
            raise Exception('calculate method must be', str(accepted_calculations))
        
    def __str__(self):
        return self.name

class DataSet:
    
    def __init__(self, data, components):
        self.data = data
        self.components = []
        if len(components) > 0:
            for component in components:
                self.components.append(component)
        
    def add_component(self, component):
        self.components.append(component)
        
    @property
    def unique_id(self):
        return 'unique_id'
    
    @property
    def id_column(self):
        return self.data[self.unique_id]
    
    @staticmethod
    def create_unique_id(row):
        return ('_').join([str(row['FACILITY_ID']), str(row['CLIENT_ID']), str(row['HOME_RMVL_KEY'])])
        
    @property
    def base_df(self):
        self.data[self.unique_id] = self.data.apply(lambda x: self.create_unique_id(x), axis=1)
        base_df = pd.DataFrame(data=self.data.ix[:, self.unique_id])
        base_df.columns = [self.unique_id]
        return base_df.drop_duplicates(self.unique_id)
    
    @property
    def interim_df(self):
        """
        Used for debugging purposes
        """
        data = self.data
        data[self.unique_id] = data.apply(lambda x: self.create_unique_id(x), axis=1)
        return data

        
    @staticmethod
    def restructure_column_names(df, component):
        new_names = [component.name + '_' + str(column) for column in df.columns.values]
        df.columns = new_names
        return df
    
    def get_totals(self, component):
        import pdb
        grouped = self.data.groupby(self.unique_id).count()[component.name]
        grouped = pd.DataFrame(index=grouped.index, data=grouped)
        grouped.columns = [component.name + '_count']
        return grouped
    
    
    def get_dummy(self, component):
        crosstab = pd.crosstab(self.id_column, self.data[component.name])
        new_names = [component.name + '_' + str(column) for column in crosstab.columns.values]
        crosstab.columns = new_names
        return crosstab
    
    def get_average(self, component):
        grouped = self.data.groupby(self.unique_id).mean()[component.name]
        grouped = pd.DataFrame(index=grouped.index, data=grouped)
        grouped.columns = [component.name + '_avg']
        return grouped
    
    def get_sum(self, component):
        grouped = self.data.groupby(self.unique_id).sum()[component.name]
        grouped = pd.DataFrame(index=grouped.index, data=grouped)
        grouped.columns = [component.name + '_sum']
        return grouped
    
    def run_calculation(self, component):
        if component.calculate == 'average':
            calc = self.get_average(component)
        elif component.calculate == 'total':
            calc = self.get_totals(component)
        elif component.calculate == 'dummy':
            calc = self.get_dummy(component)
        elif component.calculate == 'sum':
            calc = self.get_sum(component)
        else:
            raise Exception('calculations for {comp} component not supported'.format(comp=component.name))
        return calc
    
    @staticmethod
    def outcome_function(row, desirability):
        desirability = desirability[0].upper() + desirability[1:]
        if row['desirability_spell'] == desirability:
            return 1
        else:
            return 0

    def create_outcome_var(self):
        return self.data.apply(lambda x: self.outcome_function(x, 'good'), axis=1)

    def finalize_df(self):
        data = self.base_df
        for component in self.components:
            print('working on', component.name)
            calc = self.run_calculation(component)
            data = pd.merge(data, calc, left_on=self.unique_id, right_index=True, how='left')
        data.columns = [col.replace(' ', '_').lower() for col in data.columns.values]
        data['outcome'] = self.create_outcome_var()
        return data   
   

causes = [
    'Abandonment', 'Alcohol Use/Abuse - Caretaker',
    'Alcohol Use/Abuse - Child', 'Death of Parent(s)',
    'Domestic Violence', 'Drug Use/Abuse - Caretaker',
    'Drug Use/Abuse - Child', 'Incarceration of Parent/Guardian(s)',
    "JPO Removal (Child's Behavior Problem)",
    'Mental/Emotional Injuries', 'Neglect - educational needs',
    'Neglect - hygiene/clothing needs', 'Neglect - medical needs',
    'Neglect - No/Inadequate Housing', 'Neglect - nutritional needs',
    'Neglect - supervision and safety needs',
    "Parent's inability to cope",
    'Parent lacks skills for providing care',
    'Parent not seeking BH treatment',
    'Parent not seeking BH treatmnt for child', 'Parent/Child Conflict',
    'Parent/Guardian lacks skills to provide', 'Physical Abuse',
    'Relinquishment', 'Resumption', 'Sexual Abuse', 'Truancy', 'Provider.Type', 'Capacity',
    'Willing.to.Adopt', 'Gender.Served', 'Age.Range.Served',
    'lower_age_served', 'upper_age_served', 'Family Foster Care',
    'Foster Care', 'Group Home', 'Non-Relative/Kinship',
    'Non-Relative/Non-Kinship', 'Pre-Adoptive', 'Pre-Adoptive Home',
    'Pre-Adoptive Teen Mother with Non-Dependent Child', 'Regular',
    'Regular Teen Mother with Non-Dependent Child',
    'Regular Teen Mother with Two Dependent Children',
    'Relative/Kinship', 'Residential', 'Residential / Institution',
    'Residential Treatment Facility (RTF)', 'RTF Room and Board',
    'Shelter', 'Shelter Teen Mother with Non-Dependent Child',
    'Teen Family Foster Care (ages 12-21 years)',
    'Teen mother with 2 non-dependent children',
    'Teen Mother with 2 Non-Dependent Children',
    'Teen mother with non-dependent child',
    'Teen Parent Family Foster Care (ages 12-21) plus one non-dependent child',
    'Therapeutic Foster Care']

comp_dict = {}
comp_dict['RMVL_LOS'] = 'average'
comp_dict['CASE_REF_ID'] = 'total'
comp_dict['CLIENT_ID'] = 'total'
comp_dict['GENDER'] = 'dummy'
comp_dict['RACE_GROUP'] = 'dummy'
comp_dict['RMVL_TYPE'] = 'dummy'
comp_dict['RMVL_AGE'] = 'total'
comp_dict['PLCMNT_TYPE'] = 'dummy'
comp_dict['TYPE_PLACEMENT'] = 'dummy'
comp_dict['ANALYSIS_CARETYPE'] = 'dummy'
comp_dict['NUM_SPELLS'] = 'average'
comp_dict['NUM_MOVES'] = 'average'
comp_dict['NUM_SPELLS'] = 'total'
comp_dict['NUM_MOVES'] = 'total'
comp_dict['NUM_SPELLS'] = 'sum'
comp_dict['NUM_MOVES'] = 'sum'


for cause in causes:
    comp_dict[cause] = 'total'
components = []
for key, value in comp_dict.items():
    comp = Component(key, value)
    components.append(comp)