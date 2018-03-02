workflow docker_with_a_home {

    String docker_image = "broadinstitute/cromwell-docker-test:docker-with-a-home"

    call home_sweet_home {
        input:
            docker_image = docker_image
    }
}

task home_sweet_home {

    String docker_image

    command {
        echo $HOME
    }

    runtime {
        docker: docker_image
    }

    output {
        String home_dir = read_string(stdout())
    }

}
