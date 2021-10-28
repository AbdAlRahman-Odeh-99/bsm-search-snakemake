configfile: "config.yaml"

#ruleorder: 
#    scatter > generate > merge_root > select > hist_shape > hist_weight


base_dir = config['base_dir']
code_dir = config['code_dir']
thisroot_dir = config['thisroot_dir']

mc_options = config['mc_options']
select_mc_options = config['select_mc_options']
hist_shape_mc_options = config['hist_shape_mc_options']
variations_options = config['variations']
mc_weight_variations = config['mc_weight_variations']
hist_weight_mc_options = config['hist_weight_mc_options']
hist_all_mc = config['hist_all_mc']


# Signal Options
signal_options = config['signal_options']
select_signal_options = config['select_signal_options']
hist_all_signal = config['hist_all_signal']

# Data Options
data_options = config['data_options']
select_data_options = config['select_data_options']
hist_weight_data_options = config['hist_weight_data_options']
hist_all_data = config['hist_all_data']

# All Options
all_options = config['all_options']
hist_all = config['hist_all']

# Finalizing
makews_dir = config['makews_dir']
makews_outputs = config['makews_outputs']
plot_outputs = config['plot_outputs']


wildcard_constraints:
    option = '|'.join({**mc_options,**signal_options,**data_options,**all_options}),
    suffix = '|'.join({**select_mc_options,**select_signal_options,**select_data_options}),
    shapevar = '|'.join(hist_shape_mc_options),
    shapevar_weight = '|'.join(hist_weight_mc_options),
    hist_weight_data= '|'.join(hist_weight_data_options),
    jobnumber = "\d+",
    makews_dir = '|'.join(makews_dir)


def forgeGenerateCommand(wildcards):
    option = wildcards.option
    if("mc" in option):
        nevents = mc_options[option]['nevents']
    elif("sig" in option):
        nevents = signal_options[option]['nevents']
    elif("data" in option):
        nevents = data_options[option]['nevents']
    jobnumber = wildcards.jobnumber
    return  f"""
            set -x
            source {thisroot_dir}/thisroot.sh
            python {code_dir}/generatetuple.py {option} {nevents} {base_dir}/{option}_{jobnumber}.root
            """
def forgeMergeRootCommand(wildcards):
    option = wildcards.option
    if("mc" in option):
        njobs = mc_options[option]['njobs']
    elif("sig" in option):
        njobs = signal_options[option]['njobs']
    elif("data" in option):
        njobs = data_options[option]['njobs']
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
    if("mc" in option):
        return [i+1 for i in range(mc_options[option]['njobs'])]
    elif("sig" in option):
        return [i+1 for i in range(signal_options[option]['njobs'])]
    elif("data" in option):
        return [i+1 for i in range(data_options[option]['njobs'])]
    
def forgeInputMergeRoot(wildcards):
    option = wildcards.option
    file = "{}/{}".format(wildcards.base_dir,option)
    if("mc" in option):
        njobs = mc_options[option]['njobs']
    elif("sig" in option):
        njobs = signal_options[option]['njobs']
    elif("data" in option):
        njobs = data_options[option]['njobs']
    inputs = []
    for i in range(njobs):
        inputs.append("{}_{}.root".format(file,i+1))
    return inputs
def forgeSelectCommand(wildcards,input,output):
    option = wildcards.option
    suffix = wildcards.suffix
    inputfile = input
    outputfile = output
    if("mc" in option):
        region = select_mc_options[suffix]['region']
        variation = select_mc_options[suffix]['variation']
    elif("sig" in option):
        region = select_signal_options[suffix]['region']
        variation = select_signal_options[suffix]['variation']
    elif("data" in option):
        region = select_data_options[suffix]['region']
        variation = select_data_options[suffix]['variation']
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
def forgeHistWeightCommand(wildcards,input,output):
    inputfile = input
    outputfile = output
    try:
        option = wildcards.option
        mod_opt = option
        if("mc" in option):
            weight = mc_options[option]['mcweight']
            variations = mc_weight_variations[option]
        elif("sig" in option):
            weight = signal_options[option]['mcweight']
            mod_opt = option+'nal'
            variations = variations_options[option]
    except:
        option = wildcards.hist_weight_data
        parent_type = hist_weight_data_options[option]['parent_type']
        sub_type = hist_weight_data_options[option]['sub_type']
        result = hist_weight_data_options[option]['result']
        mod_opt = result
        weight = hist_weight_data_options[option]['weight']
        variations = variations_options[parent_type]
    
    return f"""
            set -x
            source {thisroot_dir}/thisroot.sh        
            variations=$(echo {variations}|sed 's| |,|g')
            name="{mod_opt}"
            python {code_dir}/histogram.py {inputfile} {outputfile} $name {weight} $variations
        """
def forgeInputHistWeightData(wildcards):
    option = wildcards.hist_weight_data
    parent_type = hist_weight_data_options[option]['parent_type']
    sub_type = hist_weight_data_options[option]['sub_type']
    file = "{}/{}".format(wildcards.base_dir,parent_type)
    inputs = "{}_{}.root".format(file,sub_type)
    return inputs
def forgeInputMergeExplicitHistShape(wildcards):
    file = "{}/{}".format(wildcards.base_dir,wildcards.option)
    inputs = []
    for i in range(len(hist_shape_mc_options)):
        inputs.append("{}_{}_hist.root".format(file,hist_shape_mc_options[i]))
    return inputs
def forgeMergeExplicitCommand(wildcards,input,output):
    inputfiles = input
    outputfile = output
    return f"""
        set -x
        inputfiles="{inputfiles}"
        INPUTS=''
        for i in $inputfiles; do
          INPUTS="$INPUTS $(printf $i)"
        done
        source {thisroot_dir}/thisroot.sh
        hadd -f {outputfile} $INPUTS
    """
def forgeInputMergeExplicitHistAll(wildcards):
    option = wildcards.option
    file = "{}/{}".format(wildcards.base_dir,option)
    inputs = []
    if("mc" in option):
        for i in range(len(hist_all_mc)):
            inputs.append("{}_{}_hist.root".format(file,hist_all_mc[i]))
    elif("sig" in option):
        for i in range(len(hist_all_signal)):
            inputs.append("{}_{}_hist.root".format(file,hist_all_signal[i]))
    elif("data" in option):
        file = "{}".format(wildcards.base_dir)
        for i in range(len(hist_all_data)):
            inputs.append("{}/{}_hist.root".format(file,hist_all_data[i]))
    elif("all" in option):
        file = "{}".format(wildcards.base_dir)
        for i in range(len(hist_all)):
            inputs.append("{}/{}_merged_hist.root".format(file,hist_all[i]))
    return inputs

def forgeMakewsCommand(wildcards):
    base_dir  = wildcards.base_dir
    xml_dir = base_dir + "/xmldir"
    workspace_prefix = base_dir + "/results"
    data_bkg_hists = base_dir + "/all_merged_hist.root" 
    return f"""
        set -x
        source {thisroot_dir}/thisroot.sh
        python {code_dir}/makews.py {data_bkg_hists} {workspace_prefix} {xml_dir}
    """

def forgePlotCommand(wildcards):
    base_dir = wildcards.base_dir
    combined_model = wildcards.base_dir+"/results_combined_meas_model.root"
    nominal_vals = wildcards.base_dir+"/nominal_vals.yml"
    fit_results = wildcards.base_dir+"/fit_results.yml"
    prefit_plot = wildcards.base_dir+"/prefit.pdf"
    postfit_plot = wildcards.base_dir+"/postfit.pdf"
    return f"""
        set -x
        source {thisroot_dir}/thisroot.sh
        hfquickplot write-vardef {combined_model} combined {nominal_vals}
        hfquickplot plot-channel {combined_model} combined channel1 x {nominal_vals} -c qcd,mc2,mc1,signal -o {prefit_plot}
        hfquickplot fit {combined_model} combined {fit_results}
        hfquickplot plot-channel {combined_model} combined channel1 x {fit_results} -c qcd,mc2,mc1,signal -o {postfit_plot}
    """



rule all:
    input:
        ##base_dir,
        expand("{base_dir}/{option}_jobs.json",base_dir=base_dir,option=mc_options),
        ##expand("{base_dir}/{option}_{jobnumber}.root",base_dir = base_dir,option=mc_options,jobnumber=4),
        expand("{base_dir}/{option}_{jobnumber}.root",base_dir = base_dir,option='mc1',jobnumber=forgeJobnumber("mc1")),
        expand("{base_dir}/{option}_{jobnumber}.root",base_dir = base_dir,option='mc2',jobnumber=forgeJobnumber("mc2")),
        ##expand("{base_dir}/{option}.root",base_dir = base_dir,option='mc1'),
        ##expand("{base_dir}/{option}.root",base_dir = base_dir,option='mc2'),
        expand("{base_dir}/{option}.root",base_dir = base_dir,option=mc_options),
        expand("{base_dir}/{option}_{suffix}.root",base_dir = base_dir,option=mc_options,suffix=select_mc_options),
        expand("{base_dir}/{option}_{shapevar}_hist.root",base_dir = base_dir,option=mc_options,shapevar=hist_shape_mc_options),
        expand("{base_dir}/{option}_{shapevar_weight}_hist.root",base_dir = base_dir,option=mc_options,shapevar_weight=hist_weight_mc_options),
        ##expand("{base_dir}/{option}_{shapevar_weight}_hist.root",base_dir = base_dir,option=mc_options,shapevar_weight=hist_weight_mc_options),
        expand("{base_dir}/{option}_shape_hist.root",base_dir = base_dir,option=mc_options),
        expand("{base_dir}/{option}_merged_hist.root",base_dir = base_dir,option=mc_options),
        #-------------------------- Signal --------------------------
        # Scatter
        expand("{base_dir}/{option}_jobs.json",base_dir=base_dir,option=signal_options),
        # Generate
        expand("{base_dir}/{option}_{jobnumber}.root",base_dir = base_dir,option=signal_options,jobnumber=forgeJobnumber("sig")),
        # Merge root
        expand("{base_dir}/{option}.root",base_dir = base_dir,option=signal_options),
        # Select
        expand("{base_dir}/{option}_{suffix}.root",base_dir = base_dir,option=signal_options,suffix=select_signal_options),
        # Hist weight
        expand("{base_dir}/{option}_{shapevar_weight}_hist.root",base_dir = base_dir,option=signal_options,shapevar_weight=hist_weight_mc_options),
        # Merge all hist
        expand("{base_dir}/{option}_merged_hist.root",base_dir = base_dir,option=signal_options),
        #-------------------------- Data --------------------------
        # Scatter
        expand("{base_dir}/{option}_jobs.json",base_dir=base_dir,option=data_options),
        # Generate
        expand("{base_dir}/{option}_{jobnumber}.root",base_dir = base_dir,option=data_options,jobnumber=forgeJobnumber("data")),
        # Merge root
        expand("{base_dir}/{option}.root",base_dir = base_dir,option=data_options),
        # Select
        expand("{base_dir}/{option}_{suffix}.root",base_dir = base_dir,option=data_options,suffix=select_data_options),
        # Hist weight
        expand("{base_dir}/{hist_weight_data}_hist.root",base_dir = base_dir,hist_weight_data=hist_weight_data_options),
        # Merge all hist
        expand("{base_dir}/{option}_merged_hist.root",base_dir = base_dir,option=data_options),
        #-------------------------- Finalizing --------------------------
        # Merge all hist
        expand("{base_dir}/{option}_merged_hist.root",base_dir = base_dir,option=all_options),
        # Makews
        expand(makews_outputs, base_dir = base_dir),
        # Plot
        expand(plot_outputs, base_dir = base_dir),
        



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
        option = params.option
        if("mc" in option):
            options = mc_options
        elif("sig" in option):
            options = signal_options
        elif("data" in option):
            options = data_options
        json_object ={ option:[i+1 for i in range(options[option]['njobs'])]}
        with open(output[0],'w') as outfile:
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
rule hist_weight:
    input:
        "{base_dir}/{option}_{shapevar_weight}.root"
    output:
        "{base_dir}/{option}_{shapevar_weight}_hist.root"
    params:
        bash_command = forgeHistWeightCommand
    shell:
        """
        {params.bash_command}
        """
rule hist_weight_data:
    input:
        forgeInputHistWeightData
    output:
        "{base_dir}/{hist_weight_data}_hist.root"
    params:
        bash_command = forgeHistWeightCommand
    shell:
        """
        {params.bash_command}
        """
rule merge_hist_shape:
    input:
        forgeInputMergeExplicitHistShape
    output:
        "{base_dir}/{option}_shape_hist.root"
    params:
        bash_command = forgeMergeExplicitCommand
    shell:
        """
        {params.bash_command}
        """
rule merge_all_hist:
    input:
        forgeInputMergeExplicitHistAll
    output:
        "{base_dir}/{option}_merged_hist.root"
    params:
        bash_command = forgeMergeExplicitCommand
    shell:
        """
        {params.bash_command}
        """
        
rule makews:
    input:
        "{base_dir}/all_merged_hist.root"
    output:
        directory(makews_outputs[0]),
        makews_outputs[1:]
    params:
        bash_command = forgeMakewsCommand
    shell:
        """
        {params.bash_command}
        """
rule plot:
    input:
        makews_outputs
    output:
        plot_outputs
    params:
        bash_command = forgePlotCommand
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