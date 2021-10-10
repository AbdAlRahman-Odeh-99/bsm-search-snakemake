configfile: "config.yaml"
#ruleorder: 
#        scatter > generate > merge_root > select


base_dir = config['base_dir']
code_dir = config['code_dir']
thisroot_dir = config['thisroot_dir']

mc_options = config['mc_options']
select_mc_options = config['select_mc_options']
hist_shape_mc_options = config['hist_shape_mc_options']
variations_options = config['variations']

wildcard_constraints:
    option = '|'.join(mc_options),
    suffix = '|'.join(select_mc_options),
    shapevar = '|'.join(hist_shape_mc_options),
    jobnumber = "\d+"


def forgeGenerateCommand(wildcards):
    option = wildcards.option
    nevents = mc_options[wildcards.option]['nevents']
    jobnumber = wildcards.jobnumber
    return  f"""
            set -x
            source {thisroot_dir}/thisroot.sh
            python {code_dir}/generatetuple.py {option} {nevents} {base_dir}/{option}_{jobnumber}.root
            """

#def forgeMergeRootCommand(option):
def forgeMergeRootCommand(wildcards):
    option = wildcards.option
    njobs = mc_options[wildcards.option]['njobs']
    return  f"""
            set -x
            BASE_DIR={base_dir}
            BASE={option}
            END={njobs}
            INPUTS=''
            echo $INPUTS
            for ((c=1;c<$END+1;c++)); do
                echo before
                INPUTS="$INPUTS $BASE_DIR/$BASE""_$c.root"
                echo inside
            done
            echo Inputs: $INPUTS
            source {thisroot_dir}/thisroot.sh
            hadd -f $BASE_DIR/$BASE.root $INPUTS
            """


def forgeJobnumber(option):
    return [i+1 for i in range(mc_options[option]['njobs'])]

def forgeInputMergeRoot(wildcards):
    file = "{}/{}".format(wildcards.base_dir,wildcards.option)
    njobs = mc_options[wildcards.option]['njobs']
    inputs = []
    for i in range(njobs):
        inputs.append("{}_{}.root".format(file,i+1))
    return inputs


def forgeSelectCommand(wildcards,input,output):
    inputfile = input
    outputfile = output
    region = select_mc_options[wildcards.suffix]['region']
    variation = select_mc_options[wildcards.suffix]['variation']
    print(inputfile)
    print(outputfile)
    print(region)
    print(variation)
    return f"""
        set -x
        source {thisroot_dir}/thisroot.sh        
        python {code_dir}/select.py {inputfile} {outputfile} {region} {variation}
        """

def forgeHistShapeCommand(wildcards,input,output):
    inputfile = input
    outputfile = output
    option = wildcards.option
    shapevar = wildcards.shapevar
    weight = mc_options[wildcards.option]['mcweight']
    variations = variations_options[wildcards.option]
    return f"""
        set -x
        source {thisroot_dir}/thisroot.sh        
        variations=$(echo {variations}|sed 's| |,|g')
        name="{option}_{shapevar}"
        python {code_dir}/histogram.py {inputfile} {outputfile} {option} {weight} $variations $name    
        """
#'''
#def mergeRootRule(option):
#    file = "{}/{}".format(base_dir,option)
#    njobs = mc_options[option]['njobs']
#    inputs = []
#    for i in range(njobs):
#        inputs.append("{}_{}.root".format(file,i+1))
#    rule:
#        input:
#            inputs
#        output:
#            file+".root"
#        group:
#            option
#        params:
#            bash_command = forgeMergeRootCommand(option)
#        shell:
#            """
#            {params.bash_command}
#            """ 
#mergeRootRule("mc1")
#mergeRootRule("mc2")
#'''

rule all:
    input:
        #base_dir,
        expand("{base_dir}/{option}_jobs.json",base_dir=base_dir,option=mc_options),
        expand("{base_dir}/{option}_{jobnumber}.root",base_dir = base_dir,option='mc1',jobnumber=forgeJobnumber("mc1")),
        expand("{base_dir}/{option}_{jobnumber}.root",base_dir = base_dir,option='mc2',jobnumber=forgeJobnumber("mc2")),
        #expand("{base_dir}/{option}.root",base_dir = base_dir,option='mc1'),
        #expand("{base_dir}/{option}.root",base_dir = base_dir,option='mc2'),
        expand("{base_dir}/{option}.root",base_dir = base_dir,option=mc_options),
        expand("{base_dir}/{option}_{suffix}.root",base_dir = base_dir,option=mc_options,suffix=select_mc_options),
        expand("{base_dir}/{option}_{shapevar}_hist.root",base_dir = base_dir,option=mc_options,shapevar=hist_shape_mc_options)

    #wildcard_constraints:
    #    option = "^[a-z]+\d*$"
        

rule prepare_dir:
    output:
        directory(base_dir)
    shell:
        """
        rm -rf {output}
        mkdir -p {output}
        """

rule scatter:
    output:
        "{base_dir}/{option}_jobs.json"
    params:
        option = "{option}"
    run:
        import json
        options = mc_options
        option = params.option
        json_object = { option:[i+1 for i in range(mc_options[option]['njobs'])]}
        with open(output[0], 'w') as outfile:
            json.dump(json_object,outfile)

rule generate:
    input:
        "{base_dir}/{option}_jobs.json"
    output:
        #temp("{base_dir}/{option}_{jobnumber}.root")
        "{base_dir}/{option}_{jobnumber}.root"
    params:
        bash_command = forgeGenerateCommand
    shell:
        """
        {params.bash_command}
        """

rule merge_root:
    input:
        forgeInputMergeRoot
    output:
        "{base_dir}/{option}.root"
    params:
        bash_command = forgeMergeRootCommand
    shell:
        """
        {params.bash_command}
        """
rule select:
    input:
        "{base_dir}/{option}.root"
    output:
        "{base_dir}/{option}_{suffix}.root"
    params:
        bash_command = forgeSelectCommand
    shell:
        """
        {params.bash_command}
        """
rule hist_shape:
    input:
        "{base_dir}/{option}_{shapevar}.root"
    output:
        "{base_dir}/{option}_{shapevar}_hist.root"
    params:
        bash_command = forgeHistShapeCommand
    shell:
        """
        {params.bash_command}
        """
        
#rule merge_root:
#        input:
#            expand("{base_dir}/{option}_{jobnumber}.json",base_dir=base_dir,option="mc1",jobnumber=[1,2,3,4])
#        output:
#            "{base_dir}/{option}.root"
#        params:
#            bash_command = forgeMergeRootCommand
#        shell:
#            """
#            {params.bash_command}
#            """

#rule generate_mc2:
#    input:
#        base_dir+"/mc2_jobs.json"
#    output:
#        temp("{base_dir}/{entry_type}_{jobnumber}.json")
#    params:
#        bash_command = forgeGenerateCommand('mc2',1)
#    shell:
#        """
#        echo {params.bash_command}
#        """

#'''
#rule generate:
#    input:
#        expand("{base_dir}/{option}_jobs.json",base_dir=base_dir,option=mc_options)
#    output:
#        temp("{base_dir}/{option}_{jobnumber}.json")
#    params:
#        entry_type = "{option}",
#        nevents = lambda wildcards: mc_options[wildcards.option]["nevents"],
#        #jobnumber = lambda wildcards: str(int(wildcards.jobnumber)+1)
#        jobnumber = "{jobnumber}"
#    shell:
#        """
#        set -x
#        source {thisroot_dir}/thisroot.sh
#        python {code_dir}/generatetuple.py {params.entry_type} {params.nevents} {base_dir}/{params.entry_type}_{params.jobnumber}.root
#        """
#'''