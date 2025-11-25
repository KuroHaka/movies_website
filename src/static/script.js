document.addEventListener('DOMContentLoaded', (event) => {
    const scrollWrappers = document.querySelectorAll('.scroll-wrapper');

    scrollWrappers.forEach(wrapper => {
        const container = wrapper.querySelector('.movie-list-horizontal');
        if (!container) return;

        const updateShadows = () => {
            const scrollLeft = container.scrollLeft;
            const scrollWidth = container.scrollWidth;
            const clientWidth = container.clientWidth;
            const atStart = scrollLeft < 5; // A small tolerance
            const atEnd = scrollLeft + clientWidth >= scrollWidth - 5; // A small tolerance

            wrapper.classList.toggle('show-left-shadow', !atStart);
            wrapper.classList.toggle('show-right-shadow', !atEnd);
        };

        // Update shadows on scroll
        container.addEventListener('scroll', updateShadows);

        // Handle wheel for horizontal scroll
        container.addEventListener('wheel', (evt) => {
            if (evt.deltaY !== 0) {
                evt.preventDefault();
                container.scrollLeft += evt.deltaY;
            }
        });

        // Initial check after a short delay to ensure layout is complete
        setTimeout(updateShadows, 100);
        // Also check on window resize
        window.addEventListener('resize', updateShadows);
    });
});
